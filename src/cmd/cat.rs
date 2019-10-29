use config::{Config, Delimiter};
use csv;
use std::prelude::v1::*;
use std::vec;
use util;
use CliResult;
use IoRedef;

static USAGE: &'static str = "
Concatenates CSV data by column or by row.

When concatenating by column, the columns will be written in the same order as
the inputs given. The number of rows in the result is always equivalent to to
the minimum number of rows across all given CSV data. (This behavior can be
reversed with the '--pad' flag.)

When concatenating by row, all CSV data must have the same number of columns.
If you need to rearrange the columns or fix the lengths of records, use the
'select' or 'fixlengths' commands. Also, only the headers of the *first* CSV
data given are used. Headers in subsequent inputs are ignored. (This behavior
can be disabled with --no-headers.)

Usage:
    xsv cat rows    [options] [<input>...]
    xsv cat columns [options] [<input>...]
    xsv cat --help

cat options:
    -p, --pad              When concatenating columns, this flag will cause
                           all records to appear. It will pad each row if
                           other CSV data isn't long enough.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Note that this has no effect when
                           concatenating columns.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    cmd_rows: bool,
    cmd_columns: bool,
    arg_input: Vec<String>,
    flag_pad: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run<T: IoRedef + Clone>(argv: &[&str], ioobj: T) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    if args.cmd_rows {
        args.cat_rows(ioobj.clone())
    } else if args.cmd_columns {
        args.cat_columns(ioobj.clone())
    } else {
        unreachable!();
    }
}

impl Args {
    fn configs<T: IoRedef + Clone>(&self, ioobj: T) -> CliResult<Vec<Config<T>>> {
        util::many_configs(
            &*self.arg_input,
            self.flag_delimiter,
            self.flag_no_headers,
            ioobj,
        )
        .map_err(From::from)
    }

    fn cat_rows<T: IoRedef + Clone>(&self, ioobj: T) -> CliResult<()> {
        let mut row = csv::ByteRecord::new();
        let mut wtr = Config::new(&self.flag_output, ioobj.clone()).writer()?;
        for (i, conf) in self.configs(ioobj.clone())?.into_iter().enumerate() {
            let mut rdr = conf.reader()?;
            if i == 0 {
                conf.write_headers(&mut rdr, &mut wtr)?;
            }
            while rdr.read_byte_record(&mut row)? {
                wtr.write_byte_record(&row)?;
            }
        }
        wtr.flush().map_err(From::from)
    }

    fn cat_columns<T: IoRedef + Clone>(&self, ioobj: T) -> CliResult<()> {
        let mut wtr = Config::new(&self.flag_output, ioobj.clone()).writer()?;
        let mut rdrs = self
            .configs(ioobj.clone())?
            .into_iter()
            .map(|conf| conf.no_headers(true).reader())
            .collect::<Result<Vec<_>, _>>()?;

        // Find the lengths of each record. If a length varies, then an error
        // will occur so we can rely on the first length being the correct one.
        let mut lengths = vec![];
        for rdr in &mut rdrs {
            lengths.push(rdr.byte_headers()?.len());
        }

        let mut iters = rdrs
            .iter_mut()
            .map(|rdr| rdr.byte_records())
            .collect::<Vec<_>>();
        'OUTER: loop {
            let mut record = csv::ByteRecord::new();
            let mut num_done = 0;
            for (iter, &len) in iters.iter_mut().zip(lengths.iter()) {
                match iter.next() {
                    None => {
                        num_done += 1;
                        if self.flag_pad {
                            for _ in 0..len {
                                record.push_field(b"");
                            }
                        } else {
                            break 'OUTER;
                        }
                    }
                    Some(Err(err)) => return fail!(err),
                    Some(Ok(next)) => record.extend(&next),
                }
            }
            // Only needed when `--pad` is set.
            // When not set, the OUTER loop breaks when the shortest iterator
            // is exhausted.
            if num_done >= iters.len() {
                break 'OUTER;
            }
            wtr.write_byte_record(&record)?;
        }
        wtr.flush().map_err(From::from)
    }
}
