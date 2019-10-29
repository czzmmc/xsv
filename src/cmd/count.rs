use config::{Config, Delimiter};
use csv;
use std::prelude::v1::*;
use std::vec;
use util;
use CliResult;

static USAGE: &'static str = "
Prints a count of the number of records in the CSV data.

Note that the count will not include the header row (unless --no-headers is
given).

Usage:
    xsv count [options] [<input>]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be included in
                           the count.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
    -o, --output <file>    Write output to <file> instead of stdout.
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_output: Option<String>,
}
use IoRedef;
pub fn run<T: IoRedef + ?Sized>(argv: &[&str], ioobj: &T) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input, ioobj)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let count = match conf.indexed()? {
        Some(idx) => idx.count(),
        None => {
            let mut rdr = conf.reader()?;
            let mut count = 0u64;
            let mut record = csv::ByteRecord::new();
            while rdr.read_byte_record(&mut record)? {
                count += 1;
            }
            count
        }
    };
    let mut wtr = Config::new(&args.flag_output, ioobj).writer()?;
    wtr.write_record(vec![count.to_string()])?;
    wtr.flush()?;
    Ok(())
}
