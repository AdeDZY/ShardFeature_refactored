class TermFeature(object):
    def __init__(self, sdf, stf, slm):
        self.sdf = sdf    # shard document frequency
        self.stf = stf    # shard term frequency
        self.slm = slm    # shard language model score


class ShardFeature(object):
    def __init__(self):
        self.shard_size = 0
        self.shard_tf = 0
        self.term2feat = {}


class FieldStats:
    def __init__(self, map_shard_features):
        self.map_shard_features = map_shard_features
        self.ref = {}
        self.ref_dv = {}
        self.get_ref()
        self.get_ref_dv()

    def get_ref(self):
        self.ref = {}  # tf / total_tf
        nterms = 0
        for shard, shard_feat in self.map_shard_features.items():
            nterms += shard_feat.shard_tf
            for term, term_feat in shard_feat.term2feat.items():
                self.ref[term] = self.ref.get(term, 0.0) + float(term_feat.stf)
        for term in self.ref:
            self.ref[term] /= float(nterms)

    def get_ref_dv(self):
        self.ref_dv = {}  # average of all doc vectors
        ndocs = 0
        for shard, shard_feat in self.map_shard_features.items():
            ndocs += shard_feat.shard_size
            for term, term_feat in shard_feat.term2feat().items():
                self.ref_dv[term] = self.ref_dv.get(term, 0.0) + term_feat.slm * shard_feat.shard_size
        for term in self.ref_dv:
            self.ref_dv[term] /= float(ndocs)