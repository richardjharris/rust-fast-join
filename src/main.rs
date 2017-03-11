#[macro_use]
extern crate clap;
use std::{io, fs};
use std::io::{BufReader, BufRead};

struct JoinFile {
    eof: bool,
    all: bool,
    field: usize,
    reader: BufReader<Box<io::Read>>,
    row: String,
    key: String,
    printed: bool,
    next_key: String,
    next_row: String,
}

fn main() {
    let (mut left, mut right) = setup();
    join(&mut left, &mut right);
}

fn join(mut left: &mut JoinFile, mut right: &mut JoinFile) {
    // We refill twice to load a current and next record
    if !(refill_first(&mut left) && refill(&mut left)) {
        panic!("No input found on left side");
    }
    if !(refill_first(&mut right) && refill(&mut right)) {
        panic!("No input found on right side");
    }

    // Loop through the inputs
    let mut todo = true;
    while todo {
        if left.key == right.key {
            print_join(&mut left, Some(&mut right));
            todo = smart_refill(&mut left, &mut right);
        }
        else if left.key < right.key {
            if left.all && !left.printed {
                print_join(&mut left, None);
            }
            todo = refill(&mut left);
        }
        else {
            if right.all && !right.printed {
                print_join(&mut right, None);
            }
            todo = refill(&mut right);
        }
    }

    // Print the last if all (normally this would happen on refill)
    if left.all && !left.printed {
        print_join(&mut left, None);
    }
    if right.all && !right.printed {
        print_join(&mut right, None);
    }

    // Finish off the remaining unpairable lines
    if !left.eof && left.all {
        finish(&mut left);
    }
    else if !right.eof && right.all {
        finish(&mut right);
    }
}

fn finish(mut file: &mut JoinFile) {
    while refill(&mut file) {
        print_join(&mut file, None);
    }
}

fn print_join(mut file: &mut JoinFile, file2: Option<&mut JoinFile>) {
    print!("{}\t{}", file.key, file.row);
    file.printed = true;

    match file2 {
        Some(f) => { print!("\t{}", f.row); f.printed = true },
        None => {},
    };
    println!("");
}

#[inline(always)]
fn refill_first(mut file: &mut JoinFile) -> bool {
    return _refill(&mut file, true);
}
#[inline(always)]
fn refill(mut file: &mut JoinFile) -> bool {
    return _refill(&mut file, false);
}

// Refill an input file
fn _refill(mut file: &mut JoinFile, first: bool) -> bool {
    // First filling won't have next values yet
    if !first {
        if file.eof {
            return false;
        }
        // XXX can this be improved? e.g. str references
        file.row = file.next_row.clone();
        file.key = file.next_key.clone();
        file.printed = false;
    }

    file.next_row.clear();

    let bytes_read = file.reader.read_line(&mut file.next_row).expect("read error");
    if bytes_read == 0 {
        file.eof = true;
        return !first;
    }

    // Remove newline XXX needs to check first
    file.next_row.pop();

    // XXX todo: split, store key field and build a string out of the rest
    // // OR just store the split version?
    // Set next_key
    file.next_key = String::from(match file.next_row.split("\t").nth(file.field - 1) {
        Some(s) => s,
        None => "",
    });

    return true;
}

// Both left and right match, decide which one to refill first
fn smart_refill(mut left: &mut JoinFile, mut right: &mut JoinFile) -> bool {
    if left.eof {
        return refill(&mut right);
    }
    else if right.eof {
        return refill(&mut left);
    }
    else if left.next_key == right.next_key {
        return refill(&mut left) && refill(&mut right);
    }
    else if left.next_key < right.next_key {
        return refill(&mut left);
    }
    else {
        return refill(&mut right);
    }
}

fn setup() -> (JoinFile, JoinFile) {
    let args = clap_app!(rjoin =>
        (version: crate_version!())
        (author: crate_authors!())
        (about: crate_description!())
        (@arg leftField: -l --left +takes_value "Select the field to index from the left file")
        (@arg rightField: -r --right +takes_value "Select the field to index from the right file")
        (@arg leftAll: -L --("left-all") "Print all lines from the left file, even if they don't match")
        (@arg rightAll: -R --("right-all") "Print all lines from the right file, even if they don't match")
        (@arg LEFT: +required "Left file")
        (@arg RIGHT: +required "Right file")
    ).get_matches();

    let left = make_join_file(&args, "LEFT", "leftField", "leftAll");
    let right = make_join_file(&args, "RIGHT", "rightField", "rightAll");

    return (left, right)
}

fn make_join_file(args: &clap::ArgMatches, filename_field: &str, field_field: &str, all_field: &str) -> JoinFile {
    let filename = args.value_of(filename_field).unwrap();
    
    let reader: Box<io::Read> = match filename {
        "-" => Box::new(io::stdin()),
        _   => Box::new(fs::File::open(filename).expect("Unable to open file"))
    };

    return JoinFile {
        field: value_t!(args, field_field, usize).unwrap_or(1),
        all: args.is_present(all_field),
        reader: BufReader::new(reader),
        eof: false,
        printed: false,
        row: String::new(),
        key: String::new(),
        next_key: String::new(),
        next_row: String::new(),
    };
}
