#[macro_use]
extern crate clap;
extern crate rjoin;

use rjoin::JoinFile;

fn main() {
    let (mut left, mut right) = setup();
    rjoin::join(&mut left, &mut right);
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
        (@arg leftFile: +required "Left file")
        (@arg rightFile: +required "Right file")
    ).get_matches();

    let mut files = vec![];
    let dirs = vec!["left", "right"];

    for dir in dirs {
        let filename = args.value_of(format!("{}File", dir)).unwrap();
        let field = value_t!(args, format!("{}Field", dir), usize).unwrap_or(1);
        let all = args.is_present(format!("{}All", dir));

        files.push( JoinFile::new(filename, field, all) );
    }
    // return the two elements as a tuple
    (files.remove(0), files.remove(0))
}
