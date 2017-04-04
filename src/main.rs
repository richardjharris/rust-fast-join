#[macro_use]
extern crate clap;
extern crate rjoin;
use std::process;
use std::error::Error;
use std::io::Write;

use rjoin::{JoinFileConfig, JoinConfig};

fn main() {
    let mut stderr = std::io::stderr();
    let config = setup().unwrap_or_else(|err| {
        writeln!(&mut stderr, "Problem parsing arguments: {}", err)
            .expect("could not write to stderr");
        process::exit(1);
    });
    if let Err(e) = rjoin::join(config) {
        writeln!(&mut stderr, "Application error: {}", e)
            .expect("could not write to stderr");
        process::exit(1);
    }
}

fn setup() -> Result<JoinConfig, Box<Error>> {
    let args = clap_app!(rjoin =>
        (version: crate_version!())
        (author: crate_authors!())
        (about: crate_description!())
        (@arg leftField: -l --left +takes_value "Select the field to index from the left file")
        (@arg rightField: -r --right +takes_value "Select the field to index from the right file")
        (@arg leftAll: -L --("left-all") "Print all lines from the left file, even if they don't match")
        (@arg rightAll: -R --("right-all") "Print all lines from the right file, even if they don't match")
        (@arg leftFile: +required "Left file")
        (@arg rightFile: +required "Right file")
    ).get_matches();

    let mut files = vec![];
    let dirs = vec!["left", "right"];

    for dir in dirs {
        let filename = args.value_of(format!("{}File", dir)).unwrap();
        let field = value_t!(args, format!("{}Field", dir), usize).unwrap_or(1);
        let all = args.is_present(format!("{}All", dir));

        files.push( JoinFileConfig { filename: filename.into(), field: field, all: all } );
    }
    // return the two elements as a tuple
    Ok(JoinConfig { left: files.remove(0), right: files.remove(0) })
}
