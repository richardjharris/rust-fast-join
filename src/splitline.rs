// Vec<&str> of fields in a string, with string also kept as backing storage.
// Must use raw pointers as Rust doesn't storing X and &(part of X) together
struct SplitLine {
    line: String,
    fields: Vec<*const str>,
    key_field: usize,
}

impl SplitLine {
    fn new(line: String, delim: char, key_field: usize) -> Self {
        let fields : Vec<*const str> = line.split(delim).map(|x| x as *const str).collect();

        SplitLine { line, fields, key_field }
    }

    fn field(&self, index: usize) -> &str {
        unsafe { &*self.fields[index] }
    }

    fn key(&self) -> &str {
        self.field(self.key_field)
    }

    // Return an iterable collection of &str.
    fn fields(&self) -> Vec<&str> {
        self.fields.iter().map(|x| unsafe { &**x }).collect()
    }
}

// Clone requires us to clone the underlying string. We reuse the offsets
impl Clone for SplitLine {
    fn clone(&self) -> Self {
        // Get start of original line (String -> &str -> *const u8)
        let origstart : *const u8 = self.line.as_bytes().as_ptr();

        // Clone original line
        let newline = self.line.clone();

        // Generate a new set of fields by calculating the original offsets and adding
        // them to the new string's pointer.
        let newfields : Vec<*const str> = self.fields.iter().map(|ptr| {
            unsafe {
                let s : &str = &**ptr;
                let offset : usize = s.as_ptr() as usize - origstart as usize;
                newline.slice_unchecked( offset, offset + s.len() ) as *const str
            }
        }).collect();

        SplitLine { line: newline, fields: newfields, key_field: self.key_field }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let s = SplitLine::new("foo\tbar\tbaz".into(), '\t', 1);
        assert_eq!(s.field(0), "foo");
        assert_eq!(s.field(1), "bar");
        assert_eq!(s.field(2), "baz");
        assert_eq!(s.key(), "bar");

        let t = s.clone();
        assert_eq!(t.field(0), "foo");
        assert_eq!(t.field(1), "bar");
        assert_eq!(t.field(2), "baz");
        assert_eq!(t.key(), "bar");
    }
}

