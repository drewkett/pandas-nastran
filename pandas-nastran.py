
import pandas as pd

class RBE2(object):
    __slots__ = ("eid","gd","ci","gis")

    def __init__(self, eid, gd, ci, gis):
        self.eid = eid
        self.gd = gd
        self.ci = ci
        self.gis = gis

    def __repr__(self):
        return f"RBE2(eid={self.eid},gd={self.gd},ci={self.ci},gis={self.gis})"

rbes = [
    #eid, gd,  ci, gi1, gi2, gi3
    RBE2(1,1,123,[10,11,12]),
    RBE2(2,2,3,[14,12]),
    RBE2(3,3,123,[15,11,14]),
]

# This one stores each independant node in a different row
class RBE2Storage(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = []
        for rbe in rbes:
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            for g in rbe.gis:
                rows.append((rbe.eid,rbe.gd,rbe.ci,g))
            seen_eids.append(rbe.eid)

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

    def lookup_by_dgid(self, gid):
        eids = self.df[self.df.gis == gid].eid
        return [ self.lookup_by_eid(eid) for eid in eids ]

# This one stores independant nodes in a list
class RBE2Storage2(object):
    def __init__(self, rbes):
        rows = []
        seen_eids = []
        for rbe in rbes:
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gd,rbe.ci,rbe.gis))
            seen_eids.append(rbe.eid)

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
        seen_eids = []
        index_rows = []
        for rbe in rbes:
            if rbe.eid in seen_eids:
                raise Exception(f"RBE2 already exists for eid {eid}")
            rows.append((rbe.eid,rbe.gd,rbe.ci,rbe.gis))
            seen_eids.append(rbe.eid)
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


r = RBE2Storage(rbes)
r2 = RBE2Storage2(rbes)
r3 = RBE2Storage3(rbes)
print(r.lookup_by_eid(2))
print(r.lookup_by_dgid(12))
print(r2.lookup_by_eid(2))
print(r2.lookup_by_dgid(12))
print(r3.lookup_by_eid(2))
print(r3.lookup_by_dgid(12))
