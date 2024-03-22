#!/usr/bin/env python3
#
# Copyright (c) 2024 MariaDB plc
#

import argparse
import json

parser = argparse.ArgumentParser(description="Post-process CSV-formatted replay results into JSON.")
parser.add_argument("CANONICALS", help="the file with the canonical forms of the SQL")
parser.add_argument("FILE", help="the replay file to process")
parser.add_argument("-o", "--output", metavar="",
                    help="the output file where the aggregated JSON is written (stdout by default).")
parser.add_argument("--debug", default=False, action="store_true", help="enable debug output")
args = parser.parse_args()

result = {
  "queries": [],
  "qps": {
    "scale": "60s",
    "values": []
  }
}

if args.output:
  with open(args.output, "w") as f:
    json.dump(result, f)
else:
  print(json.dumps(result))
