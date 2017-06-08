#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# gen jobs for shardFeature


import argparse
import os, sys
import jobWriter
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardmaps_dir")
parser.add_argument("query_file", help="each line is a query in string")
parser.add_argument("--repo_dirs", "-r", nargs='+', help="paths to one or more indexes")
args = parser.parse_args()

base_dir = "./output/" + args.partition_name
print "results in:", base_dir


if not os.path.exists(base_dir):
    os.makedirs(base_dir)

jobs_dir = base_dir + '/jobs'
if not os.path.exists(jobs_dir):
    os.makedirs(jobs_dir)

feat_dir = base_dir + '/features'
if not os.path.exists(feat_dir):
    os.makedirs(feat_dir)

shardmaps = [f.strip() for f in listdir(args.shardmaps_dir)
             if isfile(join(args.shardmaps_dir, f))]

shardmap_file = open(base_dir + "/shards", 'w')
for f in shardmaps:
    shardmap_file.write(f + '\n')
shardmap_file.close()

print 'you have {0} indexes.'.format(len(args.repo_dirs))

for shardmap in shardmaps:
    execuatable = "./shardFeature"
    arguments = "{0} {1} {2} {3} {4} {5}".format(
                                         len(args.repo_dirs),
                                         ' '.join(args.repo_dirs),
                                         args.shardmaps_dir + '/' + shardmap,
                                         feat_dir + '/' + shardmap + '.feat',
                                         feat_dir + '/' + shardmap + '.feat_bigram',
                                         args.query_file)
    log = "/tmp/zhuyund_kmeans.log"
    out = base_dir + "/out"
    err = base_dir + "/err"

    job = jobWriter.jobGenerator(execuatable, arguments, log, err, out)
    job_file = open("{0}/{1}_feat.job".format(jobs_dir, shardmap), 'w')
    job_file.write(job)
    job_file.close()



