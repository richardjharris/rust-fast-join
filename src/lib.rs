use std::error::Error;
use std::{io, fs};
use std::io::{BufReader, BufRead};
use std::cmp::Ordering;

include!("splitline.rs");

type LineIterator = Iterator<Item=io::Result<String>>;

pub struct JoinConfig {
    pub left: JoinFileConfig,
    pub right: JoinFileConfig,
    pub output: OutputOrder,
    pub output_fn: fn(String) -> (),
    pub delim: String,
    pub has_header: bool,
}

#[derive(Debug,Clone)]
pub enum KeyField {
    Indexed(usize),
    Named(String),
}
pub type KeyFields = Vec<KeyField>;

pub struct JoinFileConfig {
    pub all: bool,
    pub key_fields: KeyFields,
    pub filename: String,
    pub missing: String,
}

#[derive(Debug)]
pub enum OutputField {
    JoinField,
    // File should be 1 or 2; field should be 0-indexed
    FileField { file: usize, field: usize },
    NamedFileField { file: usize, field: String },
}

#[derive(Debug)]
pub enum OutputOrder {
    // Key, plus all other fields from file1, then file2 (GNU Join default)
    GnuDefault,
    // Similar except the same number of fields are output for each line
    Auto,
    Explicit(Vec<OutputField>),
}

struct JoinFile<'a> {
    config: &'a JoinFileConfig,
    lines: Box<LineIterator>,
    eof: bool,
    row: SplitLine,
    printed: bool,
    next_row: SplitLine,
    num_fields: usize,
    header: Option<Vec<String>>,
    key_fields: Vec<usize>,
}

impl<'a> JoinFile<'a> {
    pub fn new(config: &JoinFileConfig) -> Result<JoinFile, Box<Error>> {

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
                row: SplitLine::new("".into(), '\t', vec![]),
                next_row: SplitLine::new("".into(), '\t', vec![]),
                num_fields: 0,
                header: None,
                key_fields: vec![],
            }
        })
    }

    fn read_header(&mut self, delim: &str) -> () {
        self.header = Some(match self.lines.next() {
            Some(Ok(line)) => { line.split(&delim).map(|x| x.to_owned()).collect() },
            Some(Err(_)) => { panic!("read error") },
            None => { panic!("no header line") },
        });
    }

    // Read first two lines into row/next_row. Returns false if file is empty.
    fn first_fill(&mut self) -> bool {
        if self.read_line() {
            // Refill again to move these to .row and .key, and read in another line
            let ret = self.refill();

            // Set num_fields for the 'auto' output setting
            // XXX set to 0 if ret = false?
            self.num_fields = self.row.num_fields();
            ret
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
        match self.lines.next() {
            Some(Ok(line)) => {
                // XXX It's not good to be cloning this over and over??
                self.next_row = SplitLine::new(line, '\t', self.key_fields.clone());
                true
            },
            Some(Err(_)) => {
                panic!("read error");
            },
            None => {
                self.eof = true;
                false
            },
        }
    }
} // impl JoinFile 

pub fn join(mut config: JoinConfig) -> Result<(), Box<Error>> {
    let mut left = &mut JoinFile::new(&config.left)?;
    let mut right = &mut JoinFile::new(&config.right)?;

    if config.has_header {
        left.read_header(&config.delim);
        right.read_header(&config.delim);
    }

    if !left.first_fill() {
        panic!("No input found on left side");
    }
    if !right.first_fill() {
        panic!("No input found on right side");
    }

    // If using Auto output order, update it to Explicit now we know the
    // number of columns in each file.
    if let OutputOrder::Auto = config.output {
        let mut v = vec![];
        v.push(OutputField::JoinField);
        let mut file = 1;
        for f in vec![&left, &right] {
            for field in 0..f.num_fields {
                if let None = f.row.key_fields.iter().find(|&&i| i == field) {
                    v.push(OutputField::FileField { file, field });
                }
            }
            file += 1;
        }
        config.output = OutputOrder::Explicit(v);
    }

    // This ugly code exists to convert named fields to indexes before
    // running. Only works if we have a header
    if config.has_header {
        fn lookup_index(s: &str, v: Vec<String>) -> usize {
            match v.iter().position(|i| *i == s) {
                Some(index) => index,
                None => panic!("named column '{}' not found", s),
            }
        }

        // Convert named key/output fields to column indexes
        for f in vec![&left, &right] {
            for key in f.config.key_fields {
                if let KeyField::Named(s) = key {
                    key = KeyField::Indexed( lookup_index(&s, f.header.unwrap()) );
                }
            }
        }
        if let OutputOrder::Explicit(cols) = config.output {
            for col in cols {
                if let OutputField::NamedFileField { file, field } = col {
                    let f : *const JoinFile = if file == 1 { left } else { right };
                    col = OutputField::FileField {
                        file: file,
                        field: lookup_index(&field, unsafe { (*f).header.unwrap() }),
                    };
                }
            }
        }

        // Now we've normalized the output order, print the header
        do_header_output(&config, left, right);
    }

    // Populate f.key_fields (ugly...)
    for f in vec![&mut left, &mut right] {
        f.key_fields = f.config.key_fields.iter().map(|x| {
            match *x {
                KeyField::Indexed(s) => s,
                KeyField::Named(_) => panic!("named fields require --header"),
            }
        }).collect();
    }

    // Loop through the inputs
    let mut todo = true;
    while todo {
        match compare_keys(&left.row.keys(), &right.row.keys()) {
            Ordering::Equal => {
                do_output(&config, left, right, true, true);
                todo = smart_refill(left, right);
            },
            Ordering::Less => {
                if left.config.all && !left.printed {
                    do_output(&config, left, right, true, false);
                }
                todo = left.refill();
            },
            Ordering::Greater => {
                if right.config.all && !right.printed {
                    do_output(&config, left, right, false, true);
                }
                todo = right.refill();
            },
        };
    }

    // Print the last if all (normally this would happen on refill)
    if left.config.all && !left.printed {
        do_output(&config, left, right, true, false);
    }
    if right.config.all && !right.printed {
        do_output(&config, left, right, false, true);
    }

    // Finish off the remaining unpairable lines
    if !left.eof && left.config.all {
        while left.refill() {
            do_output(&config, left, right, true, false);
        }
    }
    else if !right.eof && right.config.all {
        while right.refill() {
            do_output(&config, left, right, false, true);
        }
    }

    Ok(())
}

fn compare_keys(left: &Vec<&str>, right: &Vec<&str>) -> Ordering {
    let mut result = Ordering::Equal;
    for i in 0..left.len() {
        result = left[i].cmp(right[i]);
        if result != Ordering::Equal {
            break
        }
    }
    result
}

fn do_header_output(config: &JoinConfig, left: &JoinFile, right: &JoinFile) {
    if let OutputOrder::Explicit(ref fields) = config.output {
        // This function is only called if headers are set.
        let left_header = left.header.as_ref().unwrap();
        let right_header = right.header.as_ref().unwrap();

        let mut cols : Vec<&str> = vec![];
        for item in fields {
            match *item {
                OutputField::JoinField => {
                    for index in &left.row.key_fields {
                        cols.push( left_header[*index].as_str() );  
                    }
                },
                OutputField::FileField { file, field } => {
                    let f = if file == 1 { left_header } else { right_header };
                    cols.push(f[field].as_str());
                },
                OutputField::NamedFileField { ref field, .. } => {
                    //XXX should this appear at all?
                    cols.push(field.as_str());
                },
            }
        }
        (config.output_fn)(cols.join(&config.delim));
    }
    else {
        panic!("gnudefault order doesn't support --header yet");
    }
}

fn do_output(config: &JoinConfig, left: &mut JoinFile, right: &mut JoinFile,
             print_left: bool, print_right: bool) {

    if print_left {
        left.printed = true;
    }
    if print_right {
        right.printed = true;
    }

    let mut keys : Vec<&str> = if print_left { left.row.keys() } else { right.row.keys() };

    let vals : Vec<&str> = match config.output {
        OutputOrder::GnuDefault => {
            // Output join field, then remaining fields from left, then right
            // Output blank fields as appropriate
            let mut v = vec![];
            v.append(&mut keys);
            if print_left {
                v.append( &mut left.row.fields_except_keys() );
            }
            if print_right {
                v.append( &mut right.row.fields_except_keys() );
            }
            v
        },
        OutputOrder::Explicit(ref fields) => {
            let mut v = vec![];
            for item in fields {
                match *item {
                    OutputField::JoinField => {
                        // XXX this might fail if key field is specified twice
                        v.append(&mut keys);
                    },
                    OutputField::FileField { file, field } => {
                        let f : *const JoinFile = if file == 1 { left } else { right };
                        v.push(unsafe {
                            if (file == 1 && print_left) || (file == 2 && print_right) {
                                // File is joined, but might still be missing a trailing field
                                if field < (*f).row.num_fields() {
                                    (*f).row.field(field)
                                }
                                else {
                                    ""
                                }
                            }
                            else {
                                // File is not joined, so use missing value
                                &(*f).config.missing
                            }
                        });
                    },
                    OutputField::NamedFileField { .. } => {
                        panic!("should have been converted to FileField");
                    },
                }
            }
            v
        },
        OutputOrder::Auto => panic!("invalid OutputOrder, this is a bug."),
    };
    (config.output_fn)(vals.join(&config.delim));

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
        match compare_keys(&left.next_row.keys(), &right.next_row.keys()) {
            Ordering::Equal => {
                left.refill() && right.refill()
            },
            Ordering::Less => { left.refill() },
            Ordering::Greater => { right.refill() },
        }
    }
}

