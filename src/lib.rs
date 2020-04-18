#![warn(rust_2018_idioms)]
#![deny(missing_docs)]

//! An in memory key value store
//!
use std::collections::HashMap;

/// A in memory key value store
#[derive(Default, Debug)]
pub struct KvStore {
    v: HashMap<String, String> 
}

impl KvStore {
    /// create a new KvStore
    pub fn new() -> Self {
        Self::default()
    }

    /// set a `key` in the store to `value`
    pub fn set(&mut self, key: String, value: String) {
        self.v.insert(key, value);
    }

    /// retrieve the value of `key`. if no value, return None
    pub fn get(&self, key: String) -> Option<String> {
        self.v.get(&key).cloned()
    }

    /// remove an entry by `key`
    pub fn remove(&mut self, key: String) {
        self.v.remove(&key);
    }
}
