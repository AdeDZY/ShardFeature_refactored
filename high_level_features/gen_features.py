#!/opt/python27/bin/python
"""
Calculate the following features.
Query Likelihood Scores L(q|s). On body, title and inlink.
Query Term Statistics {max, min} * {stf, stf*idf}. On body, title and inlink.
Bigram Log Frequency.
"""
import argparse
import os
import cent
from base import *


def read_feat_file(filepath, n_fields):
    """
    Read raw features genearted by shardFeature.cpp
    :param filepath: raw feature file path
    :return: field_features.
    """
    field_features = []
    with open(filepath) as f:
        for i in range(n_fields):
            feat = ShardFeature()
            while True:
                line = f.readline()
                if not line.strip(): # Finished reading features for this field
                    field_features.append(feat)
                    break
                t, sdf, stf, sum_prob = line.split()
                t = t.strip()
                if '-1' in t:
                    feat.shard_size = int(sdf)
                    feat.shard_tf = int(stf)
                    continue
                p = float(sum_prob) / feat.shard_size
                term_feat = TermFeature(int(sdf), int(stf), p)
                feat.term2feat[t] = term_feat
    return field_features


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("stemmed_query_file", type=argparse.FileType('r'), help="queryid:query_stem")
    parser.add_argument("stem2df_file", type=argparse.FileType('r'), help="stem df")
    parser.add_argument("--miu", "-i", type=float, default=0.0001)
    parser.add_argument("--lamb", "-l", type=float, default=500)
    parser.add_argument("--n_fields", "-f", type=int, default=3, help="number of fields. default is 3 (body, title, inlink)")

    args = parser.parse_args()

    base_dir = "./output/" + args.partition_name

    queries = []
    for query in args.stemmed_query_file:
        query = query.strip()
        query_id, query = query.split(":")
        queries.append((query_id, query))

    res_dir = base_dir + "/high_level_features/"
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)
    unigram_dir = res_dir + "/unigram/"
    if not os.path.exists(unigram_dir):
        os.makedirs(unigram_dir)
    bigram_dir = res_dir + "/bigram/"
    if not os.path.exists(bigram_dir):
        os.makedirs(bigram_dir)

    shard_file = base_dir + "/shards"
    shards = []
    for line in open(shard_file):
        shards.append(line.strip())

    # read df
    dfs = {}
    for line in args.stem2df_file:
        stem, df = line.split(' ')
        df = int(df)
        dfs[stem]  = df

    # read in all feature files

    lst_map_shard_features = [{} for i in range(args.n_fields)]
    map_shard_bigram_features = {}

    for shard in shards:
        feat_file_path = "{0}/features/{1}.feat".format(base_dir, shard)
        lst_field_features = read_feat_file(feat_file_path, n_fields=args.n_fields)
        for i in range(args.n_fields):
            lst_map_shard_features[i][shard] = lst_field_features[i]
        feat_file_path = "{0}/features/{1}.feat_bigram".format(base_dir, shard)
        bigram_features = read_feat_file(feat_file_path, n_fields=1)[0]
        map_shard_bigram_features[shard] = bigram_features

    lst_field_stats = []
    for i in range(args.n_fields):
        field_stats = FieldStats(lst_map_shard_features[i])
        lst_field_stats.append(field_stats)
    bigram_stats = FieldStats(map_shard_bigram_features)

    # output format:
    # 2 directories: unigram / bigram
    # one file per query
    # for each query_file:
    # shard_id feature_score

    for query_id, query in queries:
        unigram_outfile = open("{0}/{1}.features".format(unigram_dir, query_id), 'w')
        bigram_outfile = open("{0}/{1}.features".format(bigram_dir, query_id), 'w')
        lst_res_lm = []
        lst_res_stats = []
        for i in range(args.n_fields):
            res_lm = cent.gen_lst(lst_field_stats[i], query, dfs=dfs, method='lm', miu=args.miu)
            res_stats = cent.gen_lst(lst_field_stats[i], query, dfs=dfs, method='stats', miu=args.miu)
            lst_res_lm.append(res_lm)
            lst_res_stats.append(res_stats)
        res_bigram = cent.gen_lst_bigram(bigram_stats, query)

        for shard in shards:
            lm_scores = [str(lst_res_lm[i].get(shard, 'NAN')) for i in range(args.n_fields)]
            stats_scores = [str(lst_res_stats[i].get(shard, 'NAN')) for i in range(args.n_fields)]
            unigram_outfile.write('{0} {1} {2}\n'.format(shard, ' '.join(lm_scores), ' '.join(stats_scores)))
            bigram_outfile.write('{0} {1}\n'.format(shard, res_bigram.get(shard, 0)))
        unigram_outfile.close()
        bigram_outfile.close()


if __name__ == '__main__':
    main()