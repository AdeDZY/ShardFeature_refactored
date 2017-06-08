#!/opt/python27/bin/python

__author__ = 'zhuyund'
import argparse
import os
from os import listdir
from os.path import isfile, join
import math


def get_n_max(perc, base_dir):
    """
    get maximum number of shards to be selected
    :param perc: float, percentage of shards
    :param base_dir: str. run_base_dir
    :return: int. maximum number of shards
    """
    # get number of shards with > 1000 documents
    valid_file = open(base_dir + '/nvalid')

    n_valid_shards = int(valid_file.readline())
    valid_file.close()
    _max = int(perc * n_valid_shards)

    return _max


parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardlim", type=float, help="percentage of shards to be selected")
parser.add_argument("--miu", "-i", type=float, default=0.0001)
parser.add_argument("--method", "-m", default="lm")
parser.add_argument("--type", default="", help="bigram, ag")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name
rankings_dir = base_dir + "/rankings/"
n_max = get_n_max(args.shardlim, base_dir)

model_type = ""
if args.type:
    model_type = "_" + args.type

run_dir = "/bos/usr0/zhuyund/fedsearch/output/rankings/cent/{0}/{3}_lim{1}_miu{2}{4}/".format(args.partition_name,
                                                                                              args.shardlim,
                                                                                              args.miu,
                                                                                              args.method,
                                                                                              model_type)
if not os.path.exists(run_dir):
    os.makedirs(run_dir)

shardlist_file = open("{0}/all.shardlist".format(run_dir), 'w')

qids = set([f.strip().split('.')[0].split('_')[0] for f in listdir(rankings_dir)
            if isfile(join(rankings_dir, f)) and args.method in f])

for qid in qids:
    ranking = open("{0}/{1}_{2}.rank{3}".format(rankings_dir, qid, args.method, model_type))
    shards = []
    for i, line in enumerate(ranking):
        shard, score = line.split()
        shards.append(shard)
        if i == n_max - 1:
            break
    ranking.close()
    shardlist_file.write(qid + ' ' + ' '.join(shards))
    shardlist_file.write('\n')

shardlist_file.close()
print run_dir


