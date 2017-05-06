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

// Default handler for join output lines
fn println(s: String) -> () {
    println!("{}", s);
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
        (@arg outer: --("outer") "Print all lines from both files (equivalent to -LR)")
        (@arg leftFile: +required "Left file")
        (@arg rightFile: +required "Right file")
        (@arg leftMissing: --("left-missing") +takes_value "When using --right-all, use this value as a placeholder for any missing left columns.")
        (@arg rightMissing: --("right-missing") +takes_value "When using --left-all, use this value as a placeholder for any missing right columns.")
        (@arg output: -o --("output") +takes_value "Specify output ordering of fields (join syntax)")
    ).get_matches();

    let mut files = vec![];
    let dirs = vec!["left", "right"];
    let outer = args.is_present("outer");

    for dir in dirs {
        let filename = args.value_of(format!("{}File", dir)).unwrap();
        let mut field = value_t!(args, format!("{}Field", dir), usize).unwrap_or(1);
        let all = args.is_present(format!("{}All", dir)) || outer;
        let missing = value_t!(args, format!("{}Missing", dir), String).unwrap_or("".into());

        // Convert join field to 0 indexing
        if field < 1 {
            return Err("join field must be greater than 0".into());
        }
        field -= 1;

        files.push( JoinFileConfig { filename: filename.into(), field: field, all: all, missing: missing } );
    }

    let output = args.value_of("output").unwrap_or("gnudefault");
    let output = parse_output_fields(output)?;

    // return the two elements as a tuple
    Ok(JoinConfig {
        left: files.remove(0),
        right: files.remove(0),
        output: output,
        output_fn: println,
    })
}

// Parse a string like 'auto' or '0,1.1,1.2,2.1' into an OutputOrder struct.
fn parse_output_fields(arg: &str) -> Result<OutputOrder, Box<Error>> {

    if arg.trim() == "auto" {
        return Ok(OutputOrder::Auto)
    }
    if arg.trim() == "gnudefault" {
        return Ok(OutputOrder::GnuDefault)
    }

    let mut fields : Vec<_> = vec![];

    for item in arg.split(',') {
        let item = item.trim();

        if item == "0" {
            fields.push(OutputField::JoinField);
        }
        else {
            let nums : Vec<&str> = item.split('.').collect();
            if nums.len() != 2 {
                return Err("output field format must be '0' or 'x.y' where x is the file number and y is the field number".into());
            }
            let file = nums[0].parse::<usize>()?;
            if file != 1 && file != 2 {
                return Err("output field file number must be either 1 or 2".into());
            }
            if let Ok(field) = nums[1].parse::<usize>() {
                if field < 1 {
                    return Err("output field column number must be greater than 0".into());
                }
                // convert to 0-indexing
                fields.push(OutputField::FileField { file: file, field: field - 1 });
            }
            else {
                // Detect a 1.x-y form
                let range : Vec<&str> = nums[1].split('-').collect();
                if range.len() == 2 {
                    let start = range[0].parse::<usize>()?;
                    let end = range[1].parse::<usize>()?;
                    if start < 1 || end < 1 {
                        return Err("output field column number must be greater than 0".into());
                    }
                    if start > end {
                        let err = format!("Field range '{}' is invalid", nums[1]);
                        return Err(err.into());
                    }

                    for field in start..end+1 {
                        fields.push(OutputField::FileField { file: file, field: field - 1 });
                    }
                }
                else {
                    let err = format!("Field specification '{}' is invalid", nums[1]);
                    return Err(err.into());
                }
            }
        }
    }

    Ok(OutputOrder::Explicit(fields))
}
