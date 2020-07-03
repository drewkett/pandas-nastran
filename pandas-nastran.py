
import numpy as np
import pandas as pd
import random
import time

class RBE2(object):
    __slots__ = ("eid","gd","ci","gis")

    def __init__(self, eid, gd, ci, gis):
        self.eid = eid
        self.gd = gd
        self.ci = ci
        self.gis = gis

    def __repr__(self):
        return f"RBE2(eid={self.eid},gd={self.gd},ci={self.ci},gis={self.gis})"

#rbes = [
#    #eid, gd,  ci, gi1, gi2, gi3
#    RBE2(1,1,123,[10,11,12]),
#    RBE2(2,2,3,[14,12]),
#    RBE2(3,3,123,[15,11,14]),
#]

n = 500000
rbes = [None] * n
for i in range(n):
    rbes[i] = RBE2(i,i,123,[random.randint(0,1000),random.randint(0,1000), random.randint(0,1000)])

# This one stores each independant node in a different row
class RBE2Storage(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        for rbe in rbes:
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            for g in rbe.gis:
                rows.append((rbe.eid,rbe.gd,rbe.ci,g))
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gd","ci","gis"))

    def lookup_by_eid(self, eid):
        rows = self.df[self.df.eid == eid]
        if len(rows):
            gd = rows.iloc[0].gd
            ci = rows.iloc[0].ci
            gis = rows.gis
            return RBE2(eid, gd, ci, gis.tolist())
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_eids(self, eids):
        rows = self.df[np.in1d(self.df.eid,eids)]
        result = []
        for eid, group in rows.groupby("eid"):
            gd = group.iloc[0].gd
            ci = group.iloc[0].ci
            gis = group.gis.tolist()
            result.append(RBE2(eid,gd,ci,gis))
        return result

    def lookup_by_dgid(self, gid):
        eids = self.df[self.df.gis == gid].eid
        return self.lookup_by_eids(eids)

# This one stores independant nodes in a list
class RBE2Storage2(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gd,rbe.ci,rbe.gis))
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gd","ci","gis")).set_index("eid")

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return RBE2(eid, row.gd, row.ci, row.gis)
        else:
            raise ValueError(f"eid {eid} not found")

    # This would be slow
    def lookup_by_dgid(self, gid):
        eids = self.df[[gid in gis for gis in self.df.gis]].index
        return [ self.lookup_by_eid(eid) for eid in eids ]

# This one stores independant nodes in a list, but keeps an index
class RBE2Storage3(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = set()
        index_rows = []
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gd,rbe.ci,rbe.gis))
            seen_eids.add(rbe.eid)
            for g in rbe.gis:
                index_rows.append((g, rbe.eid))

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gd","ci","gis")).set_index("eid")
        self.gid_index = pd.DataFrame.from_records(index_rows, columns=("gid","eid")).set_index("gid")

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return RBE2(eid, row.gd, row.ci, row.gis)
        else:
            raise ValueError(f"eid {eid} not found")

    def lookup_by_dgid(self, gid):
        eids = self.gid_index.loc[gid].eid
        return [ self.lookup_by_eid(eid) for eid in eids ]

# This one stores independant nodes in a list, but also stores a copy of the original rbe object
class RBE2Storage4(object):
    def __init__(self, rbes):
        rows = [None] * len(rbes)
        seen_eids = set()
        for i, rbe in enumerate(rbes):
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows[i] = (rbe.eid,rbe.gd,rbe.ci,rbe.gis,rbe)
            seen_eids.add(rbe.eid)

        self.df = pd.DataFrame.from_records(rows, columns=("eid","gd","ci","gis","obj")).set_index("eid")

    def lookup_by_eid(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return row.obj
        else:
            raise ValueError(f"eid {eid} not found")

    # This would be slow
    def lookup_by_dgid(self, gid):
        eids = self.df[[gid in gis for gis in self.df.gis]].index
        return [ self.lookup_by_eid(eid) for eid in eids ]


t0 = time.perf_counter()
r = RBE2Storage(rbes)
t1 = time.perf_counter()
print(r)
print(r.lookup_by_eid(2))
t2 = time.perf_counter()
print(len(r.lookup_by_dgid(12)))
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
print(len(r2.lookup_by_dgid(12)))
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
print(len(r3.lookup_by_dgid(12)))
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
print(len(r4.lookup_by_dgid(12)))
t3 = time.perf_counter()
print(f"Creation   : {t1-t0} seconds")
print(f"LookupByEID: {t2-t1} seconds")
print(f"LookupByGID: {t3-t2} seconds")
