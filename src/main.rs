#[macro_use]
extern crate clap;
use std::{io, fs, mem};
use std::io::{BufReader, BufRead};

// TODO:
//
// Remove 'return's
// Investigate owning_ref
// std::mem::replace stuff
// convert some JoinFile stuff into methods
//
//   -j, -1 x, -2 x, -a/-v 1,2 options: compatible with join
//   custom delimiter -d
//   -e empty field (missing)
//   --empty-left, --empty-right: missing for joins
//   -i (ignore case) ?
//   -o format: '0,1.1,2.2'
//   -H headers (?) and -o 'Hoster Parent',...
//   --check-order ?
//   Multiple join fields support
//
//   List of pros and cons:
//     * Fixed memory usage
//     * Handles cases where one file has a huge number of rows mapping to a single row
//     * New features like multiple join fields etc
//   Cons:
//    * Doesn't handle collation other than simple byte comparison
//    * Doesn't handle (or detect) cross joins properly
//    * Doesn't detect incorrect ordering of files
//
// JoinFile::new() should not require clap args, probably

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

impl JoinFile {
    fn new(args: &clap::ArgMatches, filename_field: &str, field_field: &str, all_field: &str) -> JoinFile {
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

    // Read first two lines into row/next_row. Returns false if file is empty.
    fn first_fill(&mut self) -> bool {
        if self.read_line() {
            // Refill again to move these to .row and .key, and read in another line
            self.refill()
        }
        else {
            // Shouldn't happen for first fill
            false
        }
    }

    // Move next_row into row and read a new line. Returns false on EOF.
    fn refill(&mut self) -> bool {
        if self.eof {
            return false;
        }
        self.row = self.next_row.clone();
        self.key = self.next_key.clone();
        self.printed = false;

        // This sets .eof = true, which will cause the next call to fail.
        // XXX we actually want this to call std::mem::replace and overwrite next_row/next_key
        // with new values, return the old ones which we can then assign to row/key.
        //
        // let mut v: Vec<i32> = vec![1,2]
        // let old_v = mem::replace(&mut v, vec![3,4,5])
        self.read_line();
        return true;
    }

    // Read a line into next_row/next_key, return false on EOF
    fn read_line(&mut self) -> bool {
        self.next_row.clear();
        let bytes_read = self.reader.read_line(&mut self.next_row).expect("read error");
        if bytes_read == 0 {
            self.eof = true;
            false
        }
        else {
            self.next_row.pop();  // remove newline
            self.next_key = JoinFile::get_field(&self.next_row, self.field).into();
            true
        }
    }

    fn finish(&mut self) {
        while self.refill() {
            print_join(self, None);
        }
    }

    // Fetches 1-indexed field from row
    fn get_field<'a>(string: &'a String, field: usize) -> &'a str {
        match string.split("\t").nth(field - 1) {
            Some(s) => s,
            None => "",
        }
    }
} // impl JoinFile 

fn main() {
    let (mut left, mut right) = setup();
    join(&mut left, &mut right);
}

fn join(left: &mut JoinFile, right: &mut JoinFile) {
    if !left.first_fill() {
        panic!("No input found on left side");
    }
    if !right.first_fill() {
        panic!("No input found on right side");
    }

    // Loop through the inputs
    let mut todo = true;
    while todo {
        if left.key == right.key {
            print_join(left, Some(right));
            todo = smart_refill(left, right);
        }
        else if left.key < right.key {
            if left.all && !left.printed {
                print_join(left, None);
            }
            todo = left.refill();
        }
        else {
            if right.all && !right.printed {
                print_join(right, None);
            }
            todo = right.refill();
        }
    }

    // Print the last if all (normally this would happen on refill)
    if left.all && !left.printed {
        print_join(left, None);
    }
    if right.all && !right.printed {
        print_join(right, None);
    }

    // Finish off the remaining unpairable lines
    if !left.eof && left.all {
        left.finish();
    }
    else if !right.eof && right.all {
        right.finish();
    }
}

fn print_join(file: &mut JoinFile, file2: Option<&mut JoinFile>) {
    print!("{}\t{}", file.key, file.row);
    file.printed = true;

    match file2 {
        Some(f) => { print!("\t{}", f.row); f.printed = true },
        None => {},
    };
    println!("");
}

// Both left and right match, decide which one to refill first
fn smart_refill(left: &mut JoinFile, right: &mut JoinFile) -> bool {
    if left.eof {
        right.refill()
    }
    else if right.eof {
        left.refill()
    }
    else if left.next_key == right.next_key {
        left.refill() && right.refill()
    }
    else if left.next_key < right.next_key {
        left.refill()
    }
    else {
        right.refill()
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

    let left = JoinFile::new(&args, "LEFT", "leftField", "leftAll");
    let right = JoinFile::new(&args, "RIGHT", "rightField", "rightAll");

    (left, right)
}

