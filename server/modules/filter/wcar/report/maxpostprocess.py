#!/usr/bin/env python3
#
# Copyright (c) 2024 MariaDB plc
#

import argparse
import json
import duckdb
import numpy as np

# Reusable SQL fragments
LOW_VALUE = "MIN(duration)"
HIGH_VALUE = "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration)"
BIN_COUNT = "CASE WHEN 2 * CBRT(COUNT(duration)) < 10 THEN 10 ELSE 2 * CBRT(COUNT(duration)) END" # Rice rule: https://en.wikipedia.org/wiki/Histogram#Rice_rule
NO_ERRORS_IN_a = "COALESCE(CAST(a.error AS INT), 0) = 0"

parser = argparse.ArgumentParser(description="Post-process CSV-formatted replay results into JSON.")
parser.add_argument("CANONICALS", help="the file with the canonical forms of the SQL")
parser.add_argument("FILE", help="the replay file to process")
parser.add_argument("-o", "--output", metavar="",
                    help="the output file where the aggregated JSON is written (stdout by default).")
parser.add_argument("--debug", default=False, action="store_true", help="enable debug output")
cursor = None


def prepare_histogram_bins():
    """
    Uses numpy to size the histogram bins
    """
    global cursor
    sql_bins = f"""
  SELECT canonical,
         {LOW_VALUE} low_value,
         {HIGH_VALUE}  high_value,
         COUNT(duration) as val_count,
         {BIN_COUNT} bin_count
  FROM replay_results GROUP BY canonical
    """
    res = dict()
    for can, low_value, high_value, val_count, bin_count in cursor.execute(sql_bins).fetchall():
        range_max = high_value if val_count > 1 else low_value + 1
        counts, bins = np.histogram([], range=(low_value, range_max), bins=int(bin_count))
        res[can] = {"hist_bin_edges": bins, "hist_bin_counts": counts}
    return res


def build_histograms():
    """
    Builds the histograms by querying the bucket counts per canonical. Not all buckets have values which
    is why the bin values must first be initialied in prepare_histogram_bins().
    """
    global cursor
    hist = prepare_histogram_bins()
    sql = f"""
WITH bin_sizes AS(
  SELECT canonical,
       {LOW_VALUE} low_value,
       {HIGH_VALUE} high_value,
       {BIN_COUNT} bin_count
  FROM replay_results GROUP BY canonical
) SELECT a.canonical,
         FLOOR(((
           CASE WHEN duration < b.high_value THEN duration ELSE b.high_value END - b.low_value
         ) / COALESCE(NULLIF(b.high_value - b.low_value, 0), 1)) * b.bin_count) bin_num,
         COUNT(duration) bin_count
FROM replay_results a JOIN bin_sizes b ON (a.canonical = b.canonical)
WHERE {NO_ERRORS_IN_a}
GROUP BY 1, 2 ORDER BY 1, 2
"""
    for can, bin_num, bin_count in cursor.execute(sql).fetchall():
        counts = hist[can]["hist_bin_counts"]
        num = int(bin_num)
        counts[num if len(counts) > num else len(counts) - 1] = bin_count
    return hist


def make_stats(v_sum, v_min, v_max, v_avg, v_cnt, v_stddev):
    return {"sum": v_sum, "min": v_min, "max": v_max, "mean": v_avg, "count": v_cnt, "stddev": v_stddev}


def get_statistics(field):
    """
    Get generic statistics of the given field
    Args:
        field: The field name to count the statistics over (either `duration` or `rows_read`)
    """
    global cursor
    sql = f"""
SELECT canonical, SUM({field}), MIN({field}), MAX({field}), AVG({field}), COUNT({field}), STDDEV_POP({field})
FROM replay_results a WHERE {NO_ERRORS_IN_a} GROUP BY 1
"""
    res = dict()
    for can, v_sum, v_min, v_max, v_avg, v_cnt, v_stddev in cursor.execute(sql).fetchall():
        # v_stddev may be None if there's only one value
        res[can] = make_stats(v_sum, v_min, v_max, v_avg, v_cnt, v_stddev if v_stddev else 0)
    return res


def get_canonicals():
    """
    Get canonical IDs mapped to their SQL
    """
    global cursor
    cursor.execute(f"SELECT canonical, canonical_sql FROM canonicals")
    return {can_id: sql for can_id, sql in cursor.fetchall()}


def get_error_counts():
    """
    Get error counts per canonical
    """
    global cursor
    cursor.execute("SELECT canonical, SUM(CASE error WHEN 0 THEN 0 ELSE 1 END) FROM replay_results GROUP BY canonical")
    return {can_id: errcount for can_id, errcount in cursor.fetchall()}


def get_qps():
    """
    Get the query counts in the given resolution
    """
    global cursor
    cursor.execute("SELECT FLOOR(start_time + duration) , COUNT(*) qps FROM replay_results GROUP BY 1 ORDER BY 1")
    times, counts = zip(*cursor.fetchall())
    return {"time": list(times) + [times[-1] + 1], "counts": list(counts)}


def process(replay, canonicals):
    """
    Post-process the replay
    Args:
        replay: The replay file
        canonicals: The canonicals file

    Returns:
        The result dict with the per-canonical statistics in "queries" and the overall QPS in "qps"
    """
    global cursor
    cursor = duckdb.connect()
    cursor.execute(f"CREATE TEMPORARY TABLE replay_results AS FROM '{replay}'")
    cursor.execute(f"CREATE TEMPORARY TABLE canonicals AS FROM '{canonicals}'")

    dur_stats = get_statistics("duration")
    rr_stats = get_statistics("rows_read")
    res_stats = get_statistics("result_rows")
    canonicals = get_canonicals()
    error_counts = get_error_counts()
    qps = get_qps()
    queries = []
    no_stats = make_stats(0, 0, 0, 0, 0, 0)

    for can, h in build_histograms().items():
        queries.append({
          "id": can,
          "sql": canonicals[can],
          "errors": error_counts[can],
          "result_rows" : res_stats[can] if can in res_stats else no_stats,
          "rows_read": rr_stats[can] if can in rr_stats else no_stats,
          "duration" : (dur_stats[can] if can in dur_stats else no_stats) | {
               "hist_bin_counts": h["hist_bin_counts"].tolist(),
               "hist_bin_edges":h["hist_bin_edges"].tolist()
             }
        })

    cursor.close()
    cursor = None
    return {
      "queries": queries,
      "qps": qps
    }


def compare(res1: dict, res2: dict):
    """
    Compare the queries of two post-procesed replays
    Args:
        res1: First replay result
        res2: Second replay result

    Returns:
        A list of tuples with the query objects as the first two values and the third value as
        the difference of the statistics of the two queries, i.e. res1 - res2.
    """
    result = []
    lhs = {q["sql"]: q for q in res1["queries"]}
    rhs = {q["sql"]: q for q in res2["queries"]}
    for k in lhs:
        diff = {
            k1: {
                k2 : rhs[k][k1][k2] - lhs[k][k1][k2] for k2 in lhs[k][k1] if k2 not in ["hist_bin_counts", "hist_bin_edges"]
            } for k1 in ["duration", "rows_read", "result_rows"]
        }
        diff["errors"] = rhs[k]["errors"] - lhs[k]["errors"]
        result.append((lhs[k], rhs[k], diff))
    return result


if __name__ == '__main__':
    args = parser.parse_args()
    result = process(args.FILE, args.CANONICALS)
    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f)
    else:
        print(json.dumps(result))
