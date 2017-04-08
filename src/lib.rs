use std::error::Error;
use std::{io, fs};
use std::io::{BufReader, BufRead};
use std::cmp::Ordering;

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

type LineIterator = Iterator<Item=io::Result<String>>;

pub struct JoinConfig {
    pub left: JoinFileConfig,
    pub right: JoinFileConfig,
    pub output: OutputOrder,
}

pub struct JoinFileConfig {
    pub all: bool,
    pub field: usize,
    pub filename: String,
}

#[derive(Debug)]
pub enum OutputField {
    JoinField,
    FileField { file: usize, field: usize },
}

#[derive(Debug)]
pub enum OutputOrder {
    Auto,
    Explicit(Vec<OutputField>),
}

struct JoinFile {
    config: JoinFileConfig,
    lines: Box<LineIterator>,
    eof: bool,
    row: String,
    key: String,
    printed: bool,
    next_key: String,
    next_row: String,
}

impl JoinFile {
    pub fn new(config: JoinFileConfig) -> Result<JoinFile, Box<Error>> {

        fn open_file(filename: &str) -> Result<Box<io::Read>, Box<Error>> {
            Ok(match filename {
                "-" => Box::new(io::stdin()),
                _   => Box::new(fs::File::open(filename)?),
            })
        }

        // This error should be passed out
        open_file(&config.filename).map(|h| {
            let iter = Box::new(BufReader::new(h).lines());

            JoinFile {
                config: config,
                lines: iter,
                eof: false,
                printed: false,
                row: String::new(),
                key: String::new(),
                next_key: String::new(),
                next_row: String::new(),
            }
        })
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
        true
    }

    // Read a line into next_row/next_key, return false on EOF
    fn read_line(&mut self) -> bool {
        if let Some(line) = self.lines.next() {
            self.next_row = line.expect("read error");
            self.next_key = JoinFile::get_field(&self.next_row, self.config.field).into();
            true
        }
        else {
            self.eof = true;
            false
        }
    }

    // Fetches 1-indexed field from row
    fn get_field(string: &str, field: usize) -> &str {
        match string.split('\t').nth(field - 1) {
            Some(s) => s,
            None => "",
        }
    }
} // impl JoinFile 

pub fn join(config: JoinConfig) -> Result<(), Box<Error>> {
    let left = &mut JoinFile::new(config.left)?;
    let right = &mut JoinFile::new(config.right)?;
    let output = config.output;

    if !left.first_fill() {
        panic!("No input found on left side");
    }
    if !right.first_fill() {
        panic!("No input found on right side");
    }

    // Loop through the inputs
    let mut todo = true;
    while todo {
        match left.key.cmp(&right.key) {
            Ordering::Equal => {
                print_join(&output, Some(left), Some(right));
                todo = smart_refill(left, right);
            },
            Ordering::Less => {
                if left.config.all && !left.printed {
                    print_join(&output, Some(left), None);
                }
                todo = left.refill();
            },
            Ordering::Greater => {
                if right.config.all && !right.printed {
                    print_join(&output, None, Some(right));
                }
                todo = right.refill();
            },
        };
    }

    // Print the last if all (normally this would happen on refill)
    if left.config.all && !left.printed {
        print_join(&output, Some(left), None);
    }
    if right.config.all && !right.printed {
        print_join(&output, None, Some(right));
    }

    // Finish off the remaining unpairable lines
    if !left.eof && left.config.all {
        while left.refill() {
            print_join(&output, Some(left), None);
        }
    }
    else if !right.eof && right.config.all {
        while right.refill() {
            print_join(&output, None, Some(right));
        }
    }

    Ok(())
}

fn print_join(output: &OutputOrder, mut left: Option<&mut JoinFile>, mut right: Option<&mut JoinFile>) {
    set_printed(&mut left, &mut right);
    inner_print_join(output, &left, &right);
}

fn set_printed(left: &mut Option<&mut JoinFile>, right: &mut Option<&mut JoinFile>) {
    if let Some(ref mut f) = *left {
        f.printed = true;
    }
    if let Some(ref mut f) = *right {
        f.printed = true;
    }
}

fn inner_print_join(output: &OutputOrder, left: &Option<&mut JoinFile>, right: &Option<&mut JoinFile>) {

    let left_fields : Option<Vec<_>> = left.as_ref().map(|x| x.row.split('\t').collect());
    let right_fields : Option<Vec<_>> = right.as_ref().map(|x| x.row.split('\t').collect());
    let key : &str = left.as_ref()
                         .or_else(|| right.as_ref())
                         .unwrap().key.as_ref();

    let vals : Vec<&str> = match *output {
        OutputOrder::Auto => {
            // push key, then all non-key fields of left/right
            // this requires knowing the size of left/right
            unimplemented!();
        },
        OutputOrder::Explicit(ref fields) => {
            fields.iter().map(|item| {
                match *item {
                    OutputField::JoinField => {
                        &key
                    },
                    OutputField::FileField { file, field } => {
                        let file = if file == 1 { &left_fields } else { &right_fields };

                        match *file {
                            Some(ref f) => f[field - 1],
                            None        => "",
                        }
                    },
                }
            }).collect()
        }
    };
    println!("{}", vals.join("\t"));

}

// Both left and right match, decide which one to refill first
#[cfg_attr(feature="cargo-clippy", allow(if_same_then_else))]
fn smart_refill(left: &mut JoinFile, right: &mut JoinFile) -> bool {
    if left.eof {
        right.refill()
    }
    else if right.eof {
        left.refill()
    }
    else {
        match left.next_key.cmp(&right.next_key) {
            Ordering::Equal => {
                left.refill() && right.refill()
            },
            Ordering::Less => { left.refill() },
            Ordering::Greater => { right.refill() },
        }
    }
}
