// Vec<&str> of fields in a string, with string also kept as backing storage.
// Must use raw pointers as Rust doesn't storing X and &(part of X) together
struct SplitLine {
    line: String,
    fields: Vec<*const str>,
    // 0-indexed
    key_fields: Vec<usize>,
}

impl SplitLine {
    fn new(line: String, delim: char, key_fields: Vec<usize>) -> Self {
        let fields : Vec<*const str> = line.split(delim).map(|x| x as *const str).collect();

        if key_fields.len() == 0 {
            panic!("key_fields must have more than zero keys");
        }

        SplitLine { line, fields, key_fields }
    }

    // Return field (out of bounds?). 0-indexed
    fn field(&self, index: usize) -> &str {
        unsafe { &*self.fields[index] }
    }
    
    // XXX this should handle out of bounds by returning empty string
    fn keys(&self) -> Vec<&str> {
        self.key_fields.iter().map(|i| {
            self.field(*i)
        }).collect()
    }

    // Returns the fields
    #[allow(dead_code)]
    fn fields(&self) -> Vec<&str> {
        self.fields.iter().map(|x| unsafe { &**x }).collect()
    }

    // Returns the fields except for the key field.
    fn fields_except_keys(&self) -> Vec<&str> {
        self.fields.iter().enumerate().filter_map(|(i, x)| {
            if let Some(_) = self.key_fields.iter().find(|&&k| k == i) {
                None
            }
            else {
                unsafe { Some(&**x) }
            }
        }).collect()
    }

    // Return number of fields.
    fn num_fields(&self) -> usize {
        self.fields.len()
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

        SplitLine { line: newline, fields: newfields, key_fields: self.key_fields.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let s = SplitLine::new("foo\tbar\tbaz".into(), '\t', [1]);
        for s in [s, s.clone()] {
            assert_eq!(s.field(0), "foo");
            assert_eq!(s.field(1), "bar");
            assert_eq!(s.field(2), "baz");
            assert_eq!(s.keys(), ["bar"]);
            assert_eq!(s.num_fields(), 3);
            assert_eq!(s.fields_except_keys(), ["foo", "baz"]);
        }
    }

    #[test]
    fn empty() {
        let s = SplitLine::new("".into(), '\t', [1]);
        assert_eq!(s.num_fields(), 0);
    }

    #[test]
    fn multi_key() {
        let s = SplitLine::new("foo\tbar\tbaz".into(), '\t', [0,2]);
        assert_eq!(s.keys(), ["foo", "baz"]);
        assert_eq!(s.fields_except_keys(), ["bar"]);
        
    }
}

