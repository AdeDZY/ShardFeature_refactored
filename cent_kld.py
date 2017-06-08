#!/opt/python27/bin/python
import argparse
import numpy as np


def score_lm(qterms, feat, ref_dv, miu):
    s = []
    flag = False
    for token in qterms:
        pref = ref_dv.get(token, 0.000000000000000000001)
        if token in feat:
            flag = True 
            pcent = feat[token][2]
        else:
            pcent = 0
        pcent = (1 - miu) * pcent + miu * pref
        contri = np.log(pcent)
        s.append(contri)
    if flag:
        return sum(s)
    return 1


def score_indri(qterms, feat, ref, miu, lamb, shard_tf, shard_size):
    s = []
    for token in qterms:
        pref = ref.get(token, 0.000000000000001)
        if token in feat:
            tf = feat[token][1]
        else:
            tf = 0
        tf = float(tf)/shard_size
        p_smoothed = (1 - miu) * (tf + lamb * pref)/(float(shard_tf)/shard_size + lamb) + miu * pref
        if p_smoothed < 0:
            print shard_tf
            print tf
            print (tf + lamb * pref) 
            print p_smoothed
        s.append(np.log(p_smoothed))
    return sum(s)


def score_kld(qterms, cent, ref, miu):
    s = []
    pdoc = 1.0/(len(qterms) + 1)
    for token in qterms:
        pref = ref.get(token, 0.5)
        pcent = (1 - miu) * cent.get(token, 0) + miu * pref
        contri = pcent * np.log(pdoc / pref)
        contri += pdoc * np.log(pcent / pref)
        s.append(contri)
        # s.append( np.log(pcent) * np.log(pcent / ref[token]))
    return sum(s)


def score_ef(qterms, feat):
    s = 0
    a = {}
    for line in open("cwb.bigrams"):
        tid, freq = line.split()
        if int(freq) < 50:
            a[tid] = 1
    for item in qterms:
        token = item 
        if token in feat: 
            if token in a:
                continue
            s += np.log(float(feat[token][1]))
    return s


def score_ftr(qterms, ctf_rankings, shard):
    res = 0
    for token in qterms:
        res += 1.0/ctf_rankings[token][shard]
    return res


def stats(qterms, feat, dfs):
    tokens = []
    tokens_dfs = []
    has_all_tokens = True
    for token in qterms:
        if token not in feat:
            has_all_tokens = False
            continue
        tokens.append(token)
        tokens_dfs.append(dfs[token])

    s1, s2, s3, s4 = 0, 0, 0, 0
    s5, s6, s7, s8 = 0, 0, 0, 0
    if tokens:
        s1 = max([feat[token][1] for token in tokens])
        s3 = max([feat[token][1] * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
        s5 = max([feat[token][0] for token in tokens])
        s7 = max([feat[token][0] * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
    if tokens and has_all_tokens:
        s2 = min([feat[token][1] for token in tokens])
        s4 = min([feat[token][1] * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
        s6 = min([feat[token][0] for token in tokens])
        s8 = min([feat[token][0] * np.log(50220423.0/tokens_dfs[i]) for i, token in enumerate(tokens)])
    return s1, s2, s3, s4, s5, s6, s7, s8


def gen_lst(shards_features, ref_dv, ref, query, method, miu, lamb, shards_tf, shards_size):
    if lamb < 0:
        lamb = 25205000.0 / (len(shards_features) * 0.6) * 100 
    qterms = query.split()

    # read dfs
    dfs = {} 
    #for line in open("dfs.cwb"):
    #for line in open("dfs.mqt"):
    for line in open("dfs.gov2aol_new"):
        token, df = line.strip().split()
        df = int(df)
        dfs[token] = df

    # for ftr ranking
    ctf_rankings = {}
    for token in qterms:
        # rank shards by this token's ctf
        tmp = [(shards_features[s].get(token, (0, 0, 0))[1], s) for s in shards_features]
        tmp = sorted(tmp, reverse=True)
        ctf_rankings[token] = {}
        i = 1
        for ctf, s in tmp:
            ctf_rankings[token][s] = i
            if ctf > 0:
                i += 1

    res = []
    for shard in shards_features:
        if shards_size[shard] <= 0:
            continue
        feat = shards_features[shard]

        if method == 'kld':
            s = score_kld(qterms, feat, ref_dv, miu)
            res.append((s, shard))
        if method == "lm":
            s = score_lm(qterms, feat, ref, miu)
            if s <= 0:
                res.append((s, shard))
        if method == "indri":
            s = score_indri(qterms, feat, ref_dv, miu, lamb, shards_tf[shard], shards_size[shard])
            res.append((s, shard))
        if method == "ftr":
            s = score_ftr(qterms, ctf_rankings, shard)
            res.append((s, shard))
        if method == "ef":
            s = score_ef(qterms, feat)
            if s > 0:
                res.append((s, shard))
        if method == "stats":
            ss = stats(qterms, feat, dfs)
            res.append((" ".join([str(s) for s in ss]), shard))

    sorted_res = sorted(res, reverse=True)
    return sorted_res


def gen_lst_bigram(shards_features, ref_dv, ref, query, method, miu, lamb, shards_tf, shards_size):
    if lamb < 0:
        lamb = 25205000.0 / (len(shards_features) * 0.6) * 100
    qterms = query.split()
    qbigrams = []
    for i in range(0, len(qterms) - 1):
        gram = qterms[i] + '_' + qterms[i + 1]
        qbigrams.append(gram)

    res = []
    if not qbigrams:
        return res

    for shard in shards_features:
        if shards_size[shard] <= 0:
            continue
        feat = shards_features[shard]

        if method == 'kld':
            s = score_kld(qbigrams, feat, ref_dv, miu)
            res.append((s, shard))
        if method == "lm":
            s = score_lm(qbigrams, feat, ref, miu)
            if s <= 0:
                res.append((s, shard))
        if method == "indri":
            s = score_indri(qbigrams, feat, ref_dv, miu, lamb, shards_tf[shard], shards_size[shard])
            res.append((s, shard))
        if method == "ef":
            s = score_ef(qbigrams, feat)
            if s > 0:
                res.append((s, shard))

    sorted_res = sorted(res, reverse=True)
    return sorted_res


def merge_res(res1, res2, w1, w2):
    res = {}
    if not res1:
        return res2
    if not res2:
        return res1

    min1 = res1[-1][0] * 1.5
    min2 = res2[-1][0] * 1.5
    for s, shard in res1:
        res[shard] = s * w1 + min2 * w2
    for s, shard in res2:
        res[shard] = res.get(shard, min1 * w1) + s * w2 - min2 * w2;
    return sorted([(s, shard) for shard, s in res.items()], reverse=True)
