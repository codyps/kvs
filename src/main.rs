#![warn(rust_2018_idioms)]

use clap::{crate_authors, crate_description, crate_name, crate_version, App, Arg, SubCommand};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("KEY").index(1).required(true))
                .arg(Arg::with_name("VALUE").index(2).required(true)),
        )
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("KEY").index(1).required(true)))
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("KEY").index(1).required(true)))
        .get_matches();

    let mut kvs = kvs::KvStore::default();
    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();
            let v = sub_m.value_of("VALUE").unwrap();

            kvs.set(k.to_owned(), v.to_owned());
            println!("set: {:?} => {:?}", k, v)
        }
        ("get", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();

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
        ("rm", Some(sub_m)) => {
            let k = sub_m.value_of("KEY").unwrap();

            kvs.remove(k.to_owned());
            println!("remove: {:?}", k);
        }
        (s, _v) => {
            panic!("unknown subcommand: {}", s);
        }
    }

    Ok(())
}
