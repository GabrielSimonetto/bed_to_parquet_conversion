use std::env;
use std::process;

use btpc::Args;

fn main() {
    let config = Args::new(env::args()).unwrap_or_else(|err| {
        eprintln!("Problem parsing arguments: {}", err);
        process::exit(1);
    });

    if let Err(e) = btpc::run(config) {
        eprintln!("Application Error: {}", e);

        process::exit(1);
    }
}
