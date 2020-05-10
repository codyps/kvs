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
use std::io::{self, Seek, Write};

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
    #[snafu(display("Log entry ontains key {} instead of {} at offset {} in {}", found_key, key, offs, filename.display()))]
    LogEntryKeyMismatch {
        /// the key we wanted to find
        key: String,
        /// the key that was actually stored
        found_key: String,
        /// the offset in the file
        offs: u64,
        /// the file
        filename: PathBuf,
    },

    /// Compaction's flush failed
    #[snafu(display("Flush failed durring compaction: {}", source))]
    CompactionFlushFailed {
        /// io error
        source: io::Error,
    },

    /// Compaction's sync failed
    #[snafu(display("Sync failed durring compaction: {}", source))]
    CompactionSyncFailed {
        /// io error
        source: io::Error,
    },

    /// Compaction's rename failed
    #[snafu(display("Rename failed durring compaction: {}", source))]
    CompactionRenameFailed {
        /// io error
        source: io::Error,
    },
}

#[derive(Debug)]
#[derive(Readable, Writable)]
enum LogEntry {
    Set { key: String, value: String },
    Remove { key: String },
}

/// After 20 modifications to existing keys run compaction
const COMPACT_MODIFICATION_CT: u64 = 20;

/// result
pub type Result<T> = std::result::Result<T, KvsError>;

/// A in memory key value store
#[derive(Debug)]
pub struct KvStore {
    log_dir: PathBuf,
    log_f_name: PathBuf,
    log_f: File, 
    cache: HashMap<String, u64>,
    safe: bool,

    // track modifications to existing keys to determine when to compact
    modification_ct: u64,
}

impl KvStore {
    /// open existing or create KvStore from path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = path.into();
        let mut p = log_dir.clone();
        p.push("kvs.db");
        let log_f = fs::OpenOptions::new().create(true).read(true).write(true).open(&p)
            .context(OpenLog { filename: p.clone() })?;

        let mut cache = HashMap::new();
        let mut log_f_r = std::io::BufReader::with_capacity(8192, log_f);

        let mut modification_ct = 0;
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
                        let e = cache.entry(key);
                        if let std::collections::hash_map::Entry::Occupied(_) = e {
                            modification_ct += 1;
                        }

                        // this amounts to `e.insert(offs)`
                        e.and_modify(|v| *v = offs)
                            .or_insert(offs);
                    },
                    LogEntry::Remove { key } => {
                        modification_ct += 1;
                        cache.remove(&key);
                    }
                }

                entry_number += 1;
            }
        }

        let mut v = Self {
            log_dir,
            log_f: log_f_r.into_inner(),
            log_f_name: p,
            cache,
            safe: false,
            modification_ct,
        };

        v.maybe_compact()?;

        Ok(v)
    }

    fn maybe_compact(&mut self) -> Result<()> {
        if self.modification_ct < COMPACT_MODIFICATION_CT {
            return Ok(());
        }

        let mut tmp_path = self.log_dir.clone();
        tmp_path.push("kvs.db.tmp");

        // open a new file
        let mut tmp_log = fs::OpenOptions::new().create(true).read(true).write(true).open(&tmp_path)
            .context(OpenLog { filename: tmp_path.clone() })?;

        let mut new_cache = HashMap::with_capacity(self.cache.len());

        // write all _active_ entries to it
        // TODO: do this in disk order
        {
            let mut tmp_log_w = io::BufWriter::new(&mut tmp_log);

            for (key, offs) in self.cache.iter_mut() {
                // read from offset
                // append into new log
                self.log_f.seek(io::SeekFrom::Start(*offs))
                    .context(GetPosition { filename: self.log_f_name.clone() })?;

                let mut log_f_r = std::io::BufReader::with_capacity(8192, &mut self.log_f);
                let entry = match LogEntry::read_from_stream(&mut log_f_r) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(e).context(LogLookup { offs: *offs, filename: self.log_f_name.clone(), key: key.clone() }).into();
                    }
                };

                match entry {
                    LogEntry::Set { key: found_key, value } => {
                        if &found_key != key {
                            return Err(KvsError::LogEntryKeyMismatch { key: key.clone(), found_key, filename: self.log_f_name.clone(), offs: *offs }).into();
                        }

                        // hack to get new offset
                        let new_offs = tmp_log_w.seek(io::SeekFrom::Current(0))
                            .context(GetPosition { filename: tmp_path.clone() })?;

                        new_cache.insert(key.to_owned(), new_offs);
                        // emit data
                        LogEntry::Set { key: key.clone(), value }.write_to_stream(&mut tmp_log_w)
                            .with_context(|| LogAppendRemove { key: key.clone() })?;

                    },
                    LogEntry::Remove { key: found_key } => {
                        return Err(KvsError::LogEntryKindInvalid { offs: *offs, filename: self.log_f_name.clone(), key: key.clone(), found_key }).into();
                    }
                }
            }

            tmp_log_w.flush()
                .context(CompactionFlushFailed)?;
        }

        tmp_log.sync_all()
            .context(CompactionSyncFailed)?;

        // TODO: do some better renaming
        self.log_f = tmp_log;
        std::fs::rename(tmp_path, &self.log_f_name)
            .context(CompactionRenameFailed)?;
        self.cache = new_cache;

        Ok(())
    }

    /// set a `key` in the store to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let offs = self.log_f.seek(io::SeekFrom::End(0))
            .context(GetPosition { filename: self.log_f_name.clone() })?;

        let e = self.cache.entry(key.clone());
        if let std::collections::hash_map::Entry::Occupied(_) = e {
            self.modification_ct += 1;
        }

        e.and_modify(|v| *v = offs)
            .or_insert(offs);

        LogEntry::Set { key: key.clone(), value: value.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
            .with_context(|| LogAppendSet { key: key.clone(), value: value.clone() })?;

        // FIXME: we may have written the previous entry to the file when we didn't need to
        self.maybe_compact()?;

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

        let e = self.cache.get(&key);
        if let Some(_) = e {
            self.modification_ct += 1;
        }

        self.cache.remove(&key).ok_or(KvsError::RemoveNonexistentKey { key: key.clone() })?;

        {
            self.log_f.seek(io::SeekFrom::End(0))
                .context(GetPosition { filename: self.log_f_name.clone() })?;
            LogEntry::Remove { key: key.clone() }.write_to_stream(&mut std::io::BufWriter::new(&mut self.log_f))
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }

        // FIXME: we may have written the previous entry to the file when we didn't need to
        self.maybe_compact()?;

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }


        Ok(())
    }
}
