use hnsw_rs::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const HNSW_MAX_CONNECTIONS: usize = 16;
const HNSW_NUM_LAYERS: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;
const HNSW_EF_SEARCH: usize = 200;

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistCosine>,
    pids: Vec<String>,
    ptoi: HashMap<String, usize>,
    vectors: Vec<Vec<f32>>,
}
impl Clone for HnswIndex {
    fn clone(&self) -> Self {
        HnswIndex::build(&self.vectors, &self.pids)
            .expect("Failed to rebuild HNSW index while cloning")
    }
}

#[derive(Serialize, Deserialize)]
struct HnswIndexData {
    pids: Vec<String>,
    vectors: Vec<Vec<f32>>,
}

impl Serialize for HnswIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        HnswIndexData {
            pids: self.pids.clone(),
            vectors: self.vectors.clone(),
        }
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HnswIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = HnswIndexData::deserialize(deserializer)?;
        HnswIndex::build(&data.vectors, &data.pids).map_err(serde::de::Error::custom)
    }
}

impl HnswIndex {
    pub fn build(vectors: &[Vec<f32>], pids: &[String]) -> Result<Self, String> {
        if vectors.len() != pids.len() {
            return Err(format!(
                "vector/pid mismatch: {} vectors vs {} ids",
                vectors.len(),
                pids.len()
            ));
        }
        let hnsw = Hnsw::<f32, DistCosine>::new(
            HNSW_MAX_CONNECTIONS,
            vectors.len(),
            HNSW_NUM_LAYERS,
            HNSW_EF_CONSTRUCTION,
            DistCosine {},
        );

        for (idx, vector) in vectors.iter().enumerate() {
            hnsw.insert((vector.as_slice(), idx));
            if idx % 200 == 0 {
                println!("{idx}/{}...", vectors.len());
            }
        }

        let ptoi = pids
            .iter()
            .enumerate()
            .map(|(idx, pid)| (pid.clone(), idx))
            .collect();

        Ok(Self {
            hnsw,
            pids: pids.to_vec(),
            ptoi,
            vectors: vectors.to_vec(),
        })
    }

    pub fn insert(&mut self, pid: String, vector: Vec<f32>) -> Result<(), String> {
        if self.ptoi.contains_key(&pid) {
            return Err(format!("HNSW index already contains {pid}"));
        }
        let idx = self.vectors.len();
        self.vectors.push(vector.clone());
        self.pids.push(pid.clone());
        self.ptoi.insert(pid, idx);
        self.hnsw.insert((vector.as_slice(), idx));
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.pids.len()
    }

    pub fn find_neighbors(&self, pid: &str, k: usize) -> Option<Vec<String>> {
        let idx = self.resolve_pid(pid)?;
        let query = &self.vectors[idx];
        let neighbors = self.hnsw.search(query, k.min(self.pids.len()), HNSW_EF_SEARCH);
        let mut out = Vec::new();
        for neighbor in neighbors {
            if let Some(pid) = self.pids.get(neighbor.d_id) {
                out.push(pid.clone());
            }
        }
        Some(out)
    }

    fn resolve_pid(&self, pid: &str) -> Option<usize> {
        if let Some(idx) = self.ptoi.get(pid) {
            return Some(*idx);
        }
        self.pids.iter().position(|p| p.contains(pid))
    }
}
