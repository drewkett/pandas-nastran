
import numpy as np
import pandas as pd
import random
import time

class RBE2(object):
    __slots__ = ("eid","gm","cn","gns")

    def __init__(self, eid, gm, cn, gns):
        self.eid = eid
        self.gm = gm
        self.cn = cn
        self.gns = gns

    def __repr__(self):
        return f"RBE2(eid={self.eid},gm={self.gm},cn={self.cn},gns={self.gns})"

n = 500000
rbes = [None] * n
for i in range(n):
    rbes[i] = RBE2(i,i,123,[random.randint(0,1000),random.randint(0,1000), random.randint(0,1000)])

# This one stores each independent node in a different row
class RBE2Storage(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        for rbe in rbes:
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            for g in rbe.gns:
                rows.append((rbe.eid,rbe.gm,rbe.cn,g))
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gm","cn","gns"))

    def lookup_by_eid(self, eid):
        rows = self.df[self.df.eid == eid]
        if len(rows):
            gm = rows.iloc[0].gm
            cn = rows.iloc[0].cn
            gns = rows.gns
            return RBE2(eid, gm, cn, gns.tolist())
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_eids(self, eids):
        rows = self.df[np.in1d(self.df.eid,eids)]
        result = []
        for eid, group in rows.groupby("eid"):
            gm = group.iloc[0].gm
            cn = group.iloc[0].cn
            gns = group.gns.tolist()
            result.append(RBE2(eid,gm,cn,gns))
        return result

    def lookup_by_gn(self, gn):
        eids = self.df[self.df.gns == gn].eid
        return self.lookup_by_eids(eids)

# This one stores independent nodes in a list
class RBE2Storage2(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gm,rbe.cn,rbe.gns))
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gm","cn","gns")).set_index("eid")

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return RBE2(eid, row.gm, row.cn, row.gns)
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_eids(self, eids):
        rows = self.df.loc[eids]
        return [ RBE2(row.index, row.gm, row.cn, row.gns) for row in rows.itertuples() ]

    def lookup_by_gn(self, gn):
        eids = self.df[[gn in gns for gns in self.df.gns]].index
        return self.lookup_by_eids(eids)

# This one stores independent nodes in a list, but keeps an index
class RBE2Storage3(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        index = {}
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gm,rbe.cn,rbe.gns))
            seen_eids.add(rbe.eid)
            for g in rbe.gns:
                if g in index:
                    index[g].append(rbe.eid)
                else:
                    index[g] = [rbe.eid]

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gm","cn","gns")).set_index("eid")
        self.gn_index = index

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return RBE2(eid, row.gm, row.cn, row.gns)
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_eids(self, eids):
        rows = self.df.loc[eids]
        return [ RBE2(row.index, row.gm, row.cn, row.gns) for row in rows.itertuples() ]

    def lookup_by_gn(self, gn):
        eids = self.gn_index[gn]
        return self.lookup_by_eids(eids)

# This one stores independent nodes in a list, but also stores a copy of the original rbe object
class RBE2Storage4(object):
    def __init__(self, rbes):
        rows = [None] * len(rbes)
        seen_eids = set()
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows[i] = (rbe.eid,rbe.gm,rbe.cn,rbe.gns,rbe)
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gm","cn","gns","obj")).set_index("eid")

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return row.obj
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_eids(self, eids):
        rows = self.df.loc[eids]
        return rows.obj.tolist()

    def lookup_by_gn(self, gn):
        eids = self.df[[gn in gns for gns in self.df.gns]].index
        return self.lookup_by_eids(eids)


t0 = time.perf_counter()
r = RBE2Storage(rbes)
t1 = time.perf_counter()
print(r)
print(r.lookup_by_eid(2))
t2 = time.perf_counter()
print(len(r.lookup_by_gn(12)))
t3 = time.perf_counter()
print(f"Creation   : {t1-t0} seconds")
print(f"LookupByEID: {t2-t1} seconds")
print(f"LookupByGID: {t3-t2} seconds")

t0 = time.perf_counter()
r2 = RBE2Storage2(rbes)
t1 = time.perf_counter()
print(r2)
print(r2.lookup_by_eid(2))
t2 = time.perf_counter()
print(len(r2.lookup_by_gn(12)))
t3 = time.perf_counter()
print(f"Creation   : {t1-t0} seconds")
print(f"LookupByEID: {t2-t1} seconds")
print(f"LookupByGID: {t3-t2} seconds")

t0 = time.perf_counter()
r3 = RBE2Storage3(rbes)
t1 = time.perf_counter()
print(r3)
print(r3.lookup_by_eid(2))
t2 = time.perf_counter()
print(len(r3.lookup_by_gn(12)))
t3 = time.perf_counter()
print(f"Creation   : {t1-t0} seconds")
print(f"LookupByEID: {t2-t1} seconds")
print(f"LookupByGID: {t3-t2} seconds")

t0 = time.perf_counter()
r4 = RBE2Storage4(rbes)
t1 = time.perf_counter()
print(r4)
print(r4.lookup_by_eid(2))
t2 = time.perf_counter()
print(len(r4.lookup_by_gn(12)))
t3 = time.perf_counter()
print(f"Creation   : {t1-t0} seconds")
print(f"LookupByEID: {t2-t1} seconds")
print(f"LookupByGID: {t3-t2} seconds")
