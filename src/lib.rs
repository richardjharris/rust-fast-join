use std::error::Error;
use std::{io, fs};
use std::io::{BufReader, BufRead};

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
}

pub struct JoinFileConfig {
    pub all: bool,
    pub field: usize,
    pub filename: String,
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

    fn finish(&mut self) {
        while self.refill() {
            print_join(self, None);
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
            if left.config.all && !left.printed {
                print_join(left, None);
            }
            todo = left.refill();
        }
        else {
            if right.config.all && !right.printed {
                print_join(right, None);
            }
            todo = right.refill();
        }
    }

    // Print the last if all (normally this would happen on refill)
    if left.config.all && !left.printed {
        print_join(left, None);
    }
    if right.config.all && !right.printed {
        print_join(right, None);
    }

    // Finish off the remaining unpairable lines
    if !left.eof && left.config.all {
        left.finish();
    }
    else if !right.eof && right.config.all {
        right.finish();
    }

    Ok(())
}

fn print_join(file: &mut JoinFile, file2: Option<&mut JoinFile>) {
    print!("{}\t{}", file.key, file.row);
    file.printed = true;

    if let Some(f) = file2 {
        print!("\t{}", f.row);
        f.printed = true;
    }

    println!("");
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
