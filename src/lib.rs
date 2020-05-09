//! An in memory key value store
//!
//!
//#![warn(rust_2018_idioms)]
#![deny(missing_docs, unsafe_code)]

// Error crate options:
//  - snafu
//  - failure
//  - anyhow
//  - thiserror
//  - err-derive

use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::{self, File};

use snafu::{ResultExt, Snafu};

use speedy::{Readable, Writable};

/// error
#[derive(Debug, Snafu)]
pub enum KvsError {
    /// Open log failed
    #[snafu(display("Could not open log file at {}: {}", filename.display(), source))]
    OpenLog {
        /// filename of the log
        filename: PathBuf,
        /// Error returned by `open()`
        source: std::io::Error,
    },

    /// Log Parsing failed
    #[snafu(display("Could not read entry {}: {}", entry_number, source))]
    LogParse {
        /// log entry number
        entry_number: usize,
        /// speedy error
        source: speedy::Error,
    },

    #[cfg(feature = "capnproto")]
    /// Log Parsing failed
    #[snafu(display("Could not get root {}: {}", entry_number, source))]
    LogParseGetRoot {
        /// log entry number
        entry_number: usize,
        /// serde error
        source: capnp::Error,
    },

    /// append set failed
    #[snafu(display("Could not append Set({},{}) to log: {}", key, value, source))]
    LogAppendSet {
        /// set's Key
        key: String,
        /// set's Value
        value: String,
        /// speedy error
        source: speedy::Error,
    },

    /// append remove failed
    #[snafu(display("Could not append Rm({}) to log: {}", key, source))]
    LogAppendRemove {
        /// removes key
        key: String,
        /// speedy error
        source: speedy::Error,
    },

    /// Key not found when removing
    #[snafu(display("Key not found: {}", key))]
    RemoveNonexistentKey {
        /// removes key
        key: String,
    },

    /// Key not found when removing
    #[snafu(display("Log sync failed for {}: {}", key, source))]
    LogSync {
        /// removes key
        key: String,
        /// io error
        source: std::io::Error,
    },
}

#[derive(Debug)]
#[derive(Readable, Writable)]
enum LogEntry {
    Set { key: String, value: String },
    Remove { key: String },
}

/// result
pub type Result<T> = std::result::Result<T, KvsError>;

/// A in memory key value store
#[derive(Debug)]
pub struct KvStore {
    log_f_name: PathBuf,
    log_f: File, 
    cache: HashMap<String, String>,
    safe: bool,
}

impl KvStore {
    /// open existing or create KvStore from path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut p = path.into();
        p.push("kvs.db");
        let log_f = fs::OpenOptions::new().create(true).read(true).write(true).open(&p)
            .context(OpenLog { filename: p.clone() })?;

        let mut cache = HashMap::new();
        let mut log_f_r = std::io::BufReader::with_capacity(8192, log_f);

        {
            use speedy::IsEof;
            let mut entry_number = 0usize;
            loop {
                let entry = match LogEntry::read_from_stream(&mut log_f_r) {
                    Ok(v) => v,
                    Err(e) => {
                       if e.is_eof() {
                           break;
                       }

                       return Err(e).context(LogParse { entry_number })?;
                    }
                };

                match entry {
                    LogEntry::Set { key, value } => {
                        cache.insert(key, value);
                    },
                    LogEntry::Remove { key } => {
                        cache.remove(&key);
                    }
                }

                entry_number += 1;
            }
        }

        Ok(Self {
            log_f: log_f_r.into_inner(),
            log_f_name: p,
            cache,
            safe: false,
        })
    }

    /// set a `key` in the store to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.cache.insert(key.clone(), value.clone());

        {
            LogEntry::Set { key: key.clone(), value: value.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
                .with_context(|| LogAppendSet { key: key.clone(), value: value.clone() })?;
        }

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }
        Ok(())
    }

    /// retrieve the value of `key`. if no value, return None
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.cache.get(&key).cloned())
    }

    /// remove an entry by `key`
    pub fn remove(&mut self, key: String) -> Result<()>{
        self.cache.remove(&key).ok_or(KvsError::RemoveNonexistentKey { key: key.clone() })?;

        {
            LogEntry::Remove { key: key.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }
        Ok(())
    }
}
