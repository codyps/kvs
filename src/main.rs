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

    let mut kvs = kvs::KvStore::default();
    match opt {
        KvsOpt::Set { key, value } => {
            kvs.set(key.clone(), value.clone());
            println!("set: {:?} => {:?}", key, value)
        }
        KvsOpt::Get { key } => {
            let k = key;

            let r = kvs.get(k.to_owned());
            match r {
                None => {
                    eprintln!("key nonexistent: {:?}", k);
                    return Err("no such key".into());
                }
                Some(v) => {
                    println!("{}", v);
                }
            }
        }
        KvsOpt::Rm { key } => {
            let k = key;

            kvs.remove(k.to_owned());
            println!("remove: {:?}", k);
        }
    }

    Ok(())
}
