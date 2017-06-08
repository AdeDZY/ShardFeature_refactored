#!/opt/python27/bin/python
__author__ = 'zhuyund'

import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("--sleep", "-s", type=int, help="sleep time in seconds", default=90)
parser.add_argument("--nbatch", "-n", type=int, help="submit n batches at one time", default=1)
parser.add_argument("--start", "-t", type=int, help="start from this line of shard file", default=1)
parser.add_argument("--end", "-e", type=int, help="submit to this line of shard file", default=10000)
args = parser.parse_args()

base_dir = "./output/" + args.partition_name

n = 0
n_line = 0
for line in open(base_dir + "/shards"):
    shard = line.strip().split()[0]
    n_line += 1
    if n_line < args.start:
        continue
    if n_line > args.end:
        break
    job_path = base_dir + "/jobs/{0}_feat.job".format(shard)

    while True:
        query = "condor_q | grep " + "\"" + "shardFeature" + "\"" + "| wc -l"
        out = os.popen(query)
        nRunning = int(out.readline())
        print nRunning
        if nRunning < 12:
            break
        time.sleep(args.sleep)
    cmd = "condor_submit " + job_path
    os.system(cmd)
    n += 1
    if n % args.nbatch == 0:
        time.sleep(args.sleep)
