use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
#[derive(Clone)]
struct RBE2 {
    #[pyo3(get)]
    eid: u32,
    #[pyo3(get)]
    gm: u32,
    #[pyo3(get)]
    cn: u32,
    #[pyo3(get)]
    gns: Vec<u32>,
}

#[pymethods]
impl RBE2 {
    #[new]
    fn new(eid: u32, gm: u32, cn: u32, gns: Vec<u32>) -> Self {
        RBE2 { eid, gm, cn, gns }
    }
}

#[pyclass]
struct RBE2Storage5 {
    rbe2s: Vec<RBE2>,
    eid_index: HashMap<u32, usize>,
    gn_index: HashMap<u32, Vec<usize>>,
}

#[pymethods]
impl RBE2Storage5 {
    #[new]
    fn new(rbe2s: Vec<RBE2>) -> Self {
        let eid_index = rbe2s.iter().enumerate().map(|(i, r)| (r.eid, i)).collect();
        let mut gn_index = HashMap::new();
        for (i, rbe2) in rbe2s.iter().enumerate() {
            for gn in rbe2.gns.iter() {
                gn_index
                    .entry(*gn)
                    .and_modify(|is: &mut Vec<usize>| is.push(i))
                    .or_insert_with(|| vec![i]);
            }
        }
        Self {
            rbe2s,
            eid_index,
            gn_index,
        }
    }

    fn lookup_by_eid(&self, eid: u32) -> PyResult<RBE2> {
        match self.eid_index.get(&eid) {
            Some(i) => Ok(self.rbe2s[*i].clone()),
            None => Err(pyo3::exceptions::ValueError::py_err("argument is wrong")),
        }
    }

    fn lookup_by_gn(&self, gn: u32) -> PyResult<Vec<RBE2>> {
        match self.gn_index.get(&gn) {
            Some(ids) => Ok(ids.into_iter().map(|i| self.rbe2s[*i].clone()).collect()),
            None => Ok(vec![]),
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn rbe2rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RBE2>()?;
    m.add_class::<RBE2Storage5>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
