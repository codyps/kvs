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
use std::io::{self, Seek};

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

    /// Error determining position in file
    #[snafu(display("Could not determine offset in {}: {}", filename.display(), source))]
    GetPosition {
        /// io error
        source: std::io::Error,
        /// file we were accessing
        filename: PathBuf,
    },

    /// Looking up a previously recorded log entry failed
    #[snafu(display("Log lookup of {} in {} at offset {} failed: {}", key, filename.display(), offs, source))]
    LogLookup {
        /// Looking for the value of this key
        key: String,
        /// We had this error occur
        source: speedy::Error,
        /// in this file
        filename: PathBuf,
        /// after seeking to this offset
        offs: u64,
    },

    /// Instead of finding a LogEntry::Insert, we found some other log entry
    #[snafu(display("Log entry for {} in {} at offset {} invalid (found key {})", key, filename.display(), offs, found_key))]
    LogEntryKindInvalid {
        /// The key we were looking for
        key: String,
        /// the file
        filename: PathBuf,
        /// the offset we read from
        offs: u64,
        /// the key we found there
        found_key: String,
    },

    /// We found an insert record, but it was for the wrong key
    #[snafu(display("Log entry contains key {} instead of {} at offset {} in {}", found_key, key, offs, filename.display()))]
    LogEntryKeyMismatch {
        /// the key we wanted to find
        key: String,
        /// the key that was actually stored
        found_key: String,
        /// the offset in the file
        offs: u64,
        /// the file
        filename: PathBuf,
    }
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
    cache: HashMap<String, u64>,
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
                let offs = log_f_r.seek(io::SeekFrom::Current(0))
                    .context(GetPosition { filename: p.clone() })?;
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
                    LogEntry::Set { key, value: _ } => {
                        cache.insert(key, offs);
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
        let offs = self.log_f.seek(io::SeekFrom::End(0))
            .context(GetPosition { filename: self.log_f_name.clone() })?;
        LogEntry::Set { key: key.clone(), value: value.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
            .with_context(|| LogAppendSet { key: key.clone(), value: value.clone() })?;

        self.cache.insert(key.clone(), offs);

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }
        Ok(())
    }

    /// retrieve the value of `key`. if no value, return None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.cache.get(&key) {
            Some(&offs) => {
                self.log_f.seek(io::SeekFrom::Start(offs))
                    .context(GetPosition { filename: self.log_f_name.clone() })?;

                let mut log_f_r = std::io::BufReader::with_capacity(8192, &mut self.log_f);
                let entry = match LogEntry::read_from_stream(&mut log_f_r) {
                    Ok(v) => v,
                    Err(e) => {
                       return Err(e).context(LogLookup { offs, filename: self.log_f_name.clone(), key: key.clone() }).into();
                    }
                };

                match entry {
                    LogEntry::Set { key: found_key, value } => {
                        if found_key != key {
                            return Err(KvsError::LogEntryKeyMismatch { key: key.clone(), found_key, filename: self.log_f_name.clone(), offs }).into();
                        }

                        Ok(Some(value))
                    },
                    LogEntry::Remove { key: found_key } => {
                        return Err(KvsError::LogEntryKindInvalid { offs, filename: self.log_f_name.clone(), key: key.clone(), found_key }).into();
                    }
                }
            },
            None => {
                Ok(None)
            }
        }
    }

    /// remove an entry by `key`
    pub fn remove(&mut self, key: String) -> Result<()>{
        self.cache.remove(&key).ok_or(KvsError::RemoveNonexistentKey { key: key.clone() })?;

        {
            self.log_f.seek(io::SeekFrom::End(0))
                .context(GetPosition { filename: self.log_f_name.clone() })?;
            LogEntry::Remove { key: key.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }
        Ok(())
    }
}
