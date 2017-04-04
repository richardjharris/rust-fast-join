#[macro_use]
extern crate clap;
extern crate rjoin;
use std::process;
use std::error::Error;
use std::io::Write;

use rjoin::{JoinFileConfig, JoinConfig, OutputField, OutputOrder};

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
        (@arg output: -o --("output") +takes_value "Specify output ordering of fields (join syntax)")
    ).get_matches();

    let mut files = vec![];
    let dirs = vec!["left", "right"];

    for dir in dirs {
        let filename = args.value_of(format!("{}File", dir)).unwrap();
        let field = value_t!(args, format!("{}Field", dir), usize).unwrap_or(1);
        let all = args.is_present(format!("{}All", dir));

        files.push( JoinFileConfig { filename: filename.into(), field: field, all: all } );
    }

    let output = args.value_of("output").unwrap_or("auto");
    let output = parse_output_fields(output)?;

    // return the two elements as a tuple
    Ok(JoinConfig { left: files.remove(0), right: files.remove(0), output: output })
}

// Parse a string like 'auto' or '0,1.1,1.2,2.1' into an OutputOrder struct.
fn parse_output_fields(arg: &str) -> Result<OutputOrder, Box<Error>> {

    if arg.trim() == "auto" {
        return Ok(OutputOrder::Auto)
    }

    let mut fields : Vec<_> = vec![];

    for item in arg.split(",") {
        let item = item.trim();

        if item == "0" {
            fields.push(OutputField::JoinField);
        }
        else {
            let nums : Vec<&str> = item.split(".").collect();
            if nums.len() != 2 {
                return Err("output field format must be '0' or 'x.y' where x is the file number and y is the field number".into());
            }
            let file = nums[0].parse()?;
            if file != 1 && file != 2 {
                return Err("output field file number must be either 1 or 2".into());
            }
            let field = nums[1].parse()?;
            fields.push(OutputField::FileField { file, field });
        }
    }

    Ok(OutputOrder::Explicit(fields))
}
