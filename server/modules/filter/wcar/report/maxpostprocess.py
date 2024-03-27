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
BIN_COUNT = "2 * CBRT(COUNT(duration))" # Rice rule: https://en.wikipedia.org/wiki/Histogram#Rice_rule

parser = argparse.ArgumentParser(description="Post-process CSV-formatted replay results into JSON.")
parser.add_argument("CANONICALS", help="the file with the canonical forms of the SQL")
parser.add_argument("FILE", help="the replay file to process")
parser.add_argument("-o", "--output", metavar="",
                    help="the output file where the aggregated JSON is written (stdout by default).")
parser.add_argument("--debug", default=False, action="store_true", help="enable debug output")
args = parser.parse_args()

cursor = duckdb.connect()
cursor.execute(f"CREATE TEMPORARY TABLE replay_results AS FROM '{args.FILE}'")
cursor.execute(f"CREATE TEMPORARY TABLE canonicals AS FROM '{args.CANONICALS}'")


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
        res[can] = {"hist_bins": bins, "hist_counts": counts}
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
         FLOOR(((duration - b.low_value) / (b.high_value - b.low_value)) * b.bin_count) bin_num,
         COUNT(duration) bin_count
FROM replay_results a JOIN bin_sizes b ON (a.canonical = b.canonical)
WHERE a.duration < b.high_value AND COALESCE(CAST(a.error AS INT), 0) = 0
GROUP BY 1, 2 ORDER BY 1, 2
"""
    for can, bin_num, bin_count in cursor.execute(sql).fetchall():
        counts = hist[can]["hist_counts"]
        num = int(bin_num)
        counts[num if len(counts) > num else len(counts) - 1] = bin_count
    return hist


def get_statistics(field):
    """
    Get generic statistics of the given field
    Args:
        field: The field name to count the statistics over (either `duration` or `rows_read`)
    """
    global cursor
    sql = f"""
SELECT canonical, SUM({field}), MIN({field}), MAX({field}), AVG({field}), COUNT({field}), STDDEV({field})
FROM replay_results GROUP BY canonical
    """
    res = dict()
    for can, v_sum, v_min, v_max, v_avg, v_cnt, v_stddev in cursor.execute(sql).fetchall():
        res[can] = {"sum": v_sum, "min": v_min, "max": v_max, "mean": v_avg, "count": v_cnt, "stddev": v_stddev}
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
    cursor.execute("SELECT canonical, COUNT(error) FROM replay_results GROUP BY canonical")
    return {can_id: errcount for can_id, errcount in cursor.fetchall()}


def get_qps():
    """
    Get the query counts in the given resolution
    """
    global cursor

    val_min, val_max = cursor.execute("SELECT MIN(start_time), MAX(start_time + duration) FROM replay_results").fetchall()[0]
    num_bins = int(val_max - val_min)
    times = np.linspace(int(val_min), int(val_max) + 1, num=num_bins + 1)
    counts = [0] * num_bins
    cursor.execute("SELECT FLOOR(start_time + duration) , COUNT(*) qps FROM replay_results GROUP BY 1 ORDER BY 1")
    for time_point, qps in cursor.fetchall():
        counts[int(time_point - val_min)] = qps
    return {"time": times.tolist(), "counts": counts}


dur_stats = get_statistics("duration")
rr_stats = get_statistics("rows_read")
canonicals = get_canonicals()
error_counts = get_error_counts()
queries = []

for can, h in build_histograms().items():
    queries.append({
      "id": can,
      "sql": canonicals[can],
      "errors": error_counts[can],
      "rows_read": rr_stats[can],
      "duration" : dur_stats[can] | {
           "hist_counts": h["hist_counts"].tolist(),
           "hist_bins":h["hist_bins"].tolist()
         }
    })

result = {
  "queries": queries,
  "qps": get_qps()
}

if args.output:
  with open(args.output, "w") as f:
    json.dump(result, f)
else:
  print(json.dumps(result))
