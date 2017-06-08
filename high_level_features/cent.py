import numpy as np

def score_lm(qterms, shard_features, ref_dv, miu):
    s = []
    flag = False
    for token in qterms:
        pref = ref_dv.get(token, 0.000000000000000000001)
        if token in shard_features.term2feat:
            flag = True
            pcent = shard_features.term2feat[token].slm
        else:
            pcent = 0
        pcent = (1 - miu) * pcent + miu * pref
        contri = np.log(pcent)
        s.append(contri)
    if flag:
        return sum(s)
    return 1


def score_ef(qterms, shard_features):
    s = 0
    for item in qterms:
        token = item
        if token in shard_features.term2feat:
            s += np.log(float(shard_features.term2feat[token].stf))
    return s


def stats(qterms, shard_features, dfs):
    tokens = []
    tokens_dfs = []
    has_all_tokens = True
    for token in qterms:
        if token not in shard_features.term2feat:
            has_all_tokens = False
            continue
        tokens.append(token)
        tokens_dfs.append(dfs[token])

    s1, s2, s3, s4 = 0, 0, 0, 0

    feat = shard_features.term2feat
    if tokens:
        s1 = max([feat[token].stf for token in tokens])
        s3 = max([feat[token].stf * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
    if tokens and has_all_tokens:
        s2 = min([feat[token].stf for token in tokens])
        s4 = min([feat[token].stf * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
    return s1, s2, s3, s4


def gen_lst(field_stats, query, dfs, method, miu):

    qterms = query.split()

    res = {}
    for shard, shard_feat in field_stats.map_shard_features.items():
        if shard_feat.shard_size <= 0:
            continue

        if method == "lm":
            s = score_lm(qterms, shard_feat, field_stats.ref, miu)
            if s <= 0:
                res[shard] = s

        if method == "stats":
            ss = stats(qterms, shard_feat, dfs)
            res[shard] = " ".join([str(s) for s in ss])

    return res


def gen_lst_bigram(field_stats, query):

    qterms = query.split()
    qbigrams = []
    for i in range(0, len(qterms) - 1):
        gram = qterms[i] + '_' + qterms[i + 1]
        qbigrams.append(gram)
    res = {}
    for shard, shard_feat in field_stats.map_shard_features.items():
        if shard_feat.shard_size <= 0:
            continue

        s = score_ef(qbigrams, shard_feat)
        if s > 0:
            res[shard] = s

    return res


