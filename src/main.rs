#![warn(rust_2018_idioms)]
#![deny(unsafe_code)]
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum KvsOpt {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = KvsOpt::from_args();

    let mut kvs = kvs::KvStore::open(".")?;
    match opt {
        KvsOpt::Set { key, value } => {
            kvs.set(key.clone(), value.clone())?;
        }
        KvsOpt::Get { key } => {
            let k = key;

            let r = kvs.get(k.to_owned())?;
            match r {
                None => {
                    println!("Key not found");
                }
                Some(v) => {
                    println!("{}", v);
                }
            }
        }
        KvsOpt::Rm { key } => {
            let k = key;

            match kvs.remove(k.to_owned()) {
                Err(kvs::KvsError::RemoveNonexistentKey { key: _ }) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
                Err(e) => {
                    println!("{}", e);
                    std::process::exit(1);
                }
                Ok(_) => {},
            }
        }
    }

    Ok(())
}
