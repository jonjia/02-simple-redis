use crate::RespFrame;
use dashmap::{DashMap, DashSet};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,
    pub(crate) set_map: DashMap<String, DashSet<String>>,
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
    }
}

impl Default for BackendInner {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            hmap: DashMap::new(),
            set_map: DashMap::new(),
        }
    }
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|v| v.get(field).map(|v| v.value().clone()))
    }

    pub fn hmget(&self, key: &str, fields: &[String]) -> Vec<Option<RespFrame>> {
        let hmap = self.hmap.get(key);
        match hmap {
            Some(hmap) => fields
                .iter()
                .map(|field| hmap.get(field).map(|v| v.value().clone()))
                .collect(),
            None => vec![None; fields.len()],
        }
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let hmap = self.hmap.entry(key).or_default();
        hmap.insert(field, value);
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }

    pub fn smembers(&self, key: &str) -> Option<DashSet<String>> {
        self.set_map.get(key).map(|v| v.clone())
    }

    pub fn sadd(&self, key: &str, values: &[String]) -> i64 {
        let set = self.set_map.entry(key.to_string()).or_default();
        values
            .iter()
            .filter(|value| set.insert(value.to_string()))
            .count() as i64
    }

    pub fn sismember(&self, key: &str, value: String) -> i64 {
        let ismember = self
            .set_map
            .get(key)
            .map_or(false, |set| set.contains(&value));
        match ismember {
            true => 1,
            false => 0,
        }
    }
}
