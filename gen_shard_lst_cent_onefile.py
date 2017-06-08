#!/opt/python27/bin/python
import argparse
import os
import cent_kld


def read_feat_file(filepath):
    term2feat = {}
    shard_size = 0
    shard_tf = 0
    for line in open(filepath):
        t, df, sum_tf, sum_prob = line.split()
        t = t.strip()
        if '-1' in t :
            shard_size = int(df) 
            shard_tf = int(sum_tf)
            continue
        if shard_size == 0:
            print filepath
        p = float(sum_prob) / shard_size
        term2feat[t] = (int(df), int(sum_tf), p)
    return term2feat, shard_size, shard_tf


def get_ref(shards_features, shards_tf):
    # get reference model for smoothing
    ref = {}  # tf_in_shard / total_tf_of_shard
    nterms = 0
    for shard in shards_features:
        feat = shards_features[shard]
        shard_tf = shards_tf[shard]
        nterms += shard_tf
        for term in feat:
            df, sum_tf, sum_prob = feat[term]
            ref[term] = ref.get(term, 0.0) + float(sum_tf)
    for term in ref:
        ref[term] /= nterms
    return ref


def get_ref_dv(shards_features, shards_size):
    # get reference model for smoothing
    ref_dv = {}  # average of doc vectors
    ndocs = 0
    for shard in shards_features:
        feat = shards_features[shard]
        size = shards_size[shard]
        ndocs += size
        for term in feat:
            df, sum_tf, sum_prob = feat[term]
            ref_dv[term] = ref_dv.get(term, 0.0) + sum_prob * size
    for term in ref_dv:
        ref_dv[term] /= ndocs
    return ref_dv


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("partition_name")
    parser.add_argument("int_query_file", type=argparse.FileType('r'), help="queries in int format (queryid:queryterms)")
    parser.add_argument("outfile", type=argparse.FileType('w'))
    parser.add_argument("--method", "-m", default="lm", choices=["lm", "ef", "kld", "stats"])
    parser.add_argument("--miu", "-i", type=float, default=0.0001)
    parser.add_argument("--lamb", "-l", type=float, default=500)
    parser.add_argument("--field", "-f", type=str, default="", choices=["title", "url", "inlink"])
    parser.add_argument("--type", "-t", type=str, default="unigram", help="unigram(default), bigram")

    args = parser.parse_args()

    base_dir = "/bos/usr0/zhuyund/partition/ShardFeature/output/" + args.partition_name

    queries = []
    for query in args.int_query_file:
        query = query.strip()
        query_id, query = query.split(":")
        queries.append((query_id, query))

    res_dir = base_dir + "/rankings/"
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    shard_file = base_dir + "/shards"
    shards = []
    for line in open(shard_file):
        shards.append(line.strip())

    # read in all feature files
    shards_features = {}
    shards_size = {}
    shards_tf = {}

    shards_features_bigram = {}
    shards_tf_bigram = {}

    field = ""
    if args.field:
        field = '_' + args.field

    for shard in shards:
        feat_file_path = "{0}/features/{1}.feat{2}".format(base_dir, shard, field)
        if not os.path.exists(feat_file_path):
            shards_size[shard] = 0
            shards_tf[shard] = 0
            shards_features[shard] = {}
            continue
        feat, size, shard_tf = read_feat_file(feat_file_path)
        shards_features[shard] = feat
        shards_size[shard] = size
        shards_tf[shard] = shard_tf

        if args.type == "unigram":
            continue

        feat_file_path = "{0}/features/{1}.feat_bigram".format(base_dir, shard)
        if not os.path.exists(feat_file_path):
            shards_tf_bigram[shard] = 0
            shards_features_bigram[shard] = {}
            continue
        feat, size, shard_tf = read_feat_file(feat_file_path)
        shards_features_bigram[shard] = feat
        shards_tf_bigram[shard] = shard_tf

    # count number of shards with > 1000 documents
    n_valid_shards = len([size for size in shards_size.values() if size >= 1000])

    ref = get_ref(shards_features, shards_tf)
    ref_dv = get_ref_dv(shards_features, shards_size)

    if args.type == "bigram":
        ref_bigram = get_ref(shards_features_bigram, shards_tf_bigram)
        ref_dv_bigram = get_ref_dv(shards_features_bigram, shards_size)

    for query_id, query in queries:
        res = cent_kld.gen_lst(shards_features, ref_dv, ref, query,
                               args.method, args.miu, args.lamb,
                               shards_tf, shards_size)
        if args.type == "bigram":
            res_bigram = cent_kld.gen_lst_bigram(shards_features_bigram, ref_dv_bigram, ref_bigram, query,
                                             args.method, args.miu, args.lamb,
                                             shards_tf_bigram, shards_size)

            res_merged = cent_kld.merge_res(res, res_bigram, 0.5, 0.5)

        args.outfile.write('{0} '.format(query_id))
        for score, shard in res:
            args.outfile.write('{0} '.format(shard))
        args.outfile.write('\n')

        if args.type == "unigram":
            continue
        outfile_path = "{0}/{1}_{2}.rank_bigram".format(res_dir, query_id, args.method)
        outfile = open(outfile_path, 'w')
        for score, shard in res_bigram:
            outfile.write('{0} {1}\n'.format(shard, score))
        outfile.close()
        outfile_path = "{0}/{1}_{2}.rank_ag".format(res_dir, query_id, args.method)
        outfile = open(outfile_path, 'w')
        for score, shard in res_merged:
            outfile.write('{0} {1}\n'.format(shard, score))
        outfile.close()

    valid_file = open(base_dir + "/nvalid", 'w')
    valid_file.write(str(n_valid_shards))
    valid_file.close()

main()
