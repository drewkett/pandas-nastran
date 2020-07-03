
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

    def get_rbe(self, eid):
        rows = self.df[self.df.eid == eid]
        if len(rows):
            gd = rows.iloc[0].gd
            ci = rows.iloc[0].ci
            gis = rows.gis
            return RBE2(eid, gd, ci, gis.tolist())
        else:
            raise ValueError(f"eid {eid} not found")

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

    def get_rbe(self, eid):
        row = self.df.loc[eid]
        if len(row):
            return RBE2(eid, row.gd, row.ci, row.gis)
        else:
            raise ValueError(f"eid {eid} not found")


r = RBE2Storage(rbes)
r2 = RBE2Storage2(rbes)
print(r.get_rbe(2))
print(r2.get_rbe(2))
