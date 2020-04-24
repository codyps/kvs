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
use serde::{Serialize, Deserialize};

// capnp codegen elides these, allow it
#[cfg(feature = "capnproto")]
pub mod kvs_capnp {
  #![allow(elided_lifetimes_in_paths, missing_docs)]
  include!(concat!(env!("OUT_DIR"), "/kvs_capnp.rs"));
}

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
        /// serde error
        #[cfg(feature = "capnproto")]
        source: capnp::Error,
        /// serde error
        #[cfg(feature = "serde_cbor")]
        source: serde_cbor::error::Error,
        /// bincode error
        #[cfg(feature = "bincode")]
        source: bincode::Error,
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

    #[cfg(feature = "capnproto")]
    /// Log Parsing failed
    #[snafu(display("Could not read message {}: {}", entry_number, source))]
    LogParseReadMessage {
        /// log entry number
        entry_number: usize,
        /// serde error
        source: capnp::Error,
    },

    /// Log Parsing failed
    #[cfg(feature = "capnproto")]
    #[snafu(display("Entry Not in Schema {}: {}", entry_number, source))]
    LogParseNotInSchema {
        /// log entry number
        entry_number: usize,
        /// serde error
        source: capnp::NotInSchema,
    },

    /// append set failed
    #[snafu(display("Could not append Set({},{}) to log: {}", key, value, source))]
    LogAppendSet {
        /// set's Key
        key: String,
        /// set's Value
        value: String,
        /// serde serialization error
        #[cfg(feature = "capnproto")]
        source: std::io::Error,
        /// serde error
        #[cfg(feature = "serde_cbor")]
        source: serde_cbor::error::Error,
        /// bincode error
        #[cfg(feature = "bincode")]
        source: bincode::Error,
    },

    /// append remove failed
    #[snafu(display("Could not append Rm({}) to log: {}", key, source))]
    LogAppendRemove {
        /// removes key
        key: String,
        /// serde error
        #[cfg(feature = "capnproto")]
        source: std::io::Error,
        /// serde error
        #[cfg(feature = "serde_cbor")]
        source: serde_cbor::error::Error,
        /// bincode error
        #[cfg(feature = "bincode")]
        source: bincode::Error,
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

#[derive(Serialize, Deserialize, Debug)]
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

        // read back the log
        #[cfg(feature = "serde_cbor")]
        {
            for (entry_number, entry) in serde_cbor::Deserializer::from_reader(&mut log_f_r).into_iter().enumerate() {
                match entry {
                    Ok(LogEntry::Set { key, value }) => {
                        cache.insert(key, value);
                    },
                    Ok(LogEntry::Remove { key }) => {
                        cache.remove(&key);
                    },
                    Err(ref e) if e.is_eof() => {
                        break;
                    },
                    Err(e) => {
                        return Err(e).context(LogParse { entry_number })?;
                    }
                }
            }
        }

        #[cfg(feature = "capnproto")]
        {
            let mut entry_number = 0usize;
            loop {
                let message_reader = match capnp::serialize::read_message(&mut log_f_r, ::capnp::message::ReaderOptions::new()) {
                    Ok(v) => v,
                    Err(e) => {
                        // XXX: this is total shit
                        if e.kind == capnp::ErrorKind::Failed &&
                            e.description == "failed to fill whole buffer" {

                                break;
                        }
                        
                        return Err(e).context(LogParseReadMessage { entry_number })?;
                    }
                };
                let entry = message_reader.get_root::<kvs_capnp::entry::Reader>()
                    .context(LogParseGetRoot { entry_number })?;

                match entry.which().context(LogParseNotInSchema { entry_number })? {
                    kvs_capnp::entry::Set(set) => {
                        let set = set.context(LogParse { entry_number })?;
                        cache.insert(set.get_key().context(LogParse { entry_number })?.to_owned(),
                            set.get_value().context(LogParse { entry_number })?.to_owned());
                    },
                    kvs_capnp::entry::Rm(rm) => {
                        let rm = rm.context(LogParse { entry_number })?;
                        cache.remove(rm.get_key().context(LogParse { entry_number })?);
                    }
                }

                entry_number += 1;
            }
        }

        #[cfg(feature = "bincode")]
        {
            let mut entry_number = 0usize;
            loop {
                let entry = match bincode::deserialize_from(&mut log_f_r) {
                    Ok(v) => v,
                    Err(e) => {
                        if let bincode::ErrorKind::Io(ref io_e) = *e {
                            if io_e.kind() == std::io::ErrorKind::UnexpectedEof {
                                break;
                            }
                        }

                        return Err(e).context(LogParse { entry_number })?;
                    }
                };

                match entry {
                    LogEntry::Set { key, value } => {
                        cache.insert(key, value);
                    }
                    LogEntry::Remove { key } => {
                        cache.remove(&key);
                    }
                }

                entry_number += 1;
            }
        }

        #[cfg(not(any(
                feature = "capnproto",
                feature = "serde_cbor",
                feature = "bincode"
                )))]
        {
            NoSerializationDefined::ZZZZZ;
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
        #[cfg(feature = "capnproto")]
        {
            let mut message = ::capnp::message::Builder::new_default();
            {
                let entry = message.init_root::<kvs_capnp::entry::Builder<'_>>();
                let mut set = entry.init_set();
                set.set_key(&key);
                set.set_value(&value);
            }

            ::capnp::serialize::write_message(&mut std::io::BufWriter::new(&mut self.log_f), &message)
                .with_context(|| LogAppendSet { key: key.clone(), value: value.clone() })?;
        }

        #[cfg(feature = "serde_cbor")]
        {
            serde_cbor::to_writer(&mut std::io::BufWriter::new(&mut self.log_f), &LogEntry::Set { key: key.clone(), value: value.clone() })
                .with_context(|| LogAppendSet { key: key.clone(), value: value.clone() })?;
        }
        #[cfg(feature = "bincode")]
        {
            bincode::serialize_into(&mut std::io::BufWriter::new(&mut self.log_f), &LogEntry::Set { key: key.clone(), value: value.clone() })
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
        #[cfg(feature = "capnproto")]
        {
            let mut message = ::capnp::message::Builder::new_default();
            {
                let entry = message.init_root::<kvs_capnp::entry::Builder<'_>>();
                let mut set = entry.init_rm();
                set.set_key(&key);
            }

            ::capnp::serialize::write_message(&mut std::io::BufWriter::new(&mut self.log_f), &message)
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }
        #[cfg(feature = "serde_cbor")]
        {
            serde_cbor::to_writer(&mut std::io::BufWriter::new(&mut self.log_f), &LogEntry::Remove { key: key.clone() })
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }
        #[cfg(feature = "bincode")]
        {
            bincode::serialize_into(&mut std::io::BufWriter::new(&mut self.log_f), &LogEntry::Remove { key: key.clone() })
                .with_context(|| LogAppendRemove { key: key.clone() })?;
        }

        if self.safe {
            self.log_f.sync_all().with_context(|| LogSync { key })?;
        }
        Ok(())
    }
}
