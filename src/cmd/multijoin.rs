// use std::fmt;

use crate::cmd::common::{cmp_key, get_row_key};
use config::{Config, Delimiter};
use csv;
use select::{SelectColumns, Selection};
use std::cmp;
#[cfg(not(feature = "mesalock_sgx"))]
use std::fs;
use std::io;
use std::iter::Extend;
use std::prelude::v1::*;
use std::str;
use util;
use SeekRead;
use {CliError, CliResult};
static USAGE: &'static str = "
Joins two or more sorted sets of CSV data on the specified columns.

The default join operation is an 'inner' join. This corresponds to the
intersection of rows on the keys specified.

Joins are always done by ignoring leading and trailing whitespace. By default,
joins are done case sensitively, but this can be disabled with the --no-case
flag.

The columns arguments specify the columns to join for each input. Columns can
be referenced by name or index, starting at 1. Specify multiple columns by
separating them with a comma. Specify a range of columns with `-`. Both
Must specify exactly the same number of columns for each input.
(See 'xsv select --help' for the full syntax.)

Usage:
    xsv multijoin (--ascended|--descended) [options] (<column> <input>) (<column> <input>)...
    xsv multijoin --help

multijoin options:
    --no-case              When set, joins are done case insensitively.
    --nulls                When set, joins will work on empty fields.
                           Otherwise, empty fields are completely ignored.
                           (In fact, any row that has an empty field in the
                           key specified is ignored.)
    -U, --unique           When set, the selected column key of a table must     
                           be unique to identify the record in the table.
    -A, --ascended         If the tables have been sorted by the selected column
                           in ascending order,the flag can make it fast.
    -D, --descended        If the tables have been sorted by the selected column
                           in descending order,the flag can make it fast.
    -N, --numeric          Multiple tables are sorted by string numerical value of the 
                           specified column.(default: according to string value)
                           
Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

type ByteString = Vec<u8>;

#[derive(Deserialize)]
struct OpArgs {
    arg_column: Vec<SelectColumns>,
    arg_input: Vec<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_no_case: bool,
    flag_nulls: bool,
    flag_unique: bool,
    flag_ascended: bool,
    flag_descended: bool,
    flag_numeric: bool,
    flag_delimiter: Option<Delimiter>,
}
use IoRedef;
pub fn run<T: IoRedef + ?Sized>(argv: &[&str], ioobj: &T) -> CliResult<()> {
    let args: OpArgs = util::get_args(USAGE, argv)?;
    if !args.flag_descended && !args.flag_ascended || args.flag_ascended && args.flag_descended {
        return Err(CliError::Other("Parameter error".to_string()));
    }
    let mut state = args.new_io_state(ioobj)?;
    state.write_headers()?;
    state.inner_join()?;
    Ok(())
}

struct IoState<W: io::Write> {
    wtr: csv::Writer<W>,
    rdr: Vec<csv::Reader<Box<dyn SeekRead>>>,
    sel: Vec<Selection>,
    no_headers: bool,
    flag_unique: bool,
    flag_ascended: bool,
    flag_descended: bool,
    flag_numeric: bool,
    casei: bool,
    nulls: bool,
}

impl<W: io::Write> IoState<W> {
    fn write_headers(&mut self) -> CliResult<()> {
        if !self.no_headers {
            let mut headers = self.rdr[0].byte_headers()?.clone();
            let mut i = 1;
            while i < self.rdr.len() {
                headers.extend(self.rdr[i].byte_headers()?.iter());
                i += 1;
            }

            self.wtr.write_record(&headers)?;
        }
        Ok(())
    }

    fn inner_join(mut self) -> CliResult<()> {
        let mut tmp_key: Option<Vec<ByteString>>= None;
        while let Some(row) = self.rdr[0].byte_records().next() {
            let row = row?;
            let key = get_row_key(&self.sel[0], &row, self.casei);
            if !self.nulls && key.iter().any(|f| f.is_empty()) {
                continue;
            }
            if tmp_key == Some(key.clone()) && self.flag_unique {
                continue;
            }
            if tmp_key.is_some(){
                match (self.flag_ascended,cmp_key(&tmp_key.unwrap(), &key, self.flag_numeric)){
                    (true,cmp::Ordering::Greater)=>return Err(CliError::Other("Not in ascending order".to_string())),
                    (false,cmp::Ordering::Less)=>return Err(CliError::Other("Not in descending order".to_string())),
                    _=>{},
                }
            }

            tmp_key = Some(key.clone());
            self.inner_join_operator(&key, &row, 1)?;
        }
        Ok(())
    }

    fn inner_join_operator(
        &mut self,
        key: &[ByteString],
        row: &csv::ByteRecord,
        flag_loop: usize,
    ) -> CliResult<()> {
        let mut pos = csv::Position::new();
        let mut first_eq_pos = csv::Position::new();
        let mut tmp_key: Vec<ByteString> = Vec::new();
        let count = self.rdr.len() - 1;
        let mut eq_num = 0;
        loop {
            // Read the position immediately before each record.
            let next_pos = self.rdr[flag_loop].position().clone();
            if let Some(row2) = self.rdr[flag_loop].byte_records().next() {
                let row2 = row2?;
                tmp_key = get_row_key(&self.sel[flag_loop], &row2, self.casei);
                match cmp_key(&tmp_key, key, self.flag_numeric) {
                    cmp::Ordering::Equal => {
                        if eq_num == 0 {
                            first_eq_pos = next_pos.clone();
                        }
                        eq_num += 1;
                        let mut tmp_row = row.clone();
                        tmp_row.extend(row2.iter());
                        if flag_loop == count {
                            self.wtr.write_record(tmp_row.iter())?;
                        } else {
                            self.inner_join_operator(key, &tmp_row, flag_loop + 1)?;
                        }

                        if self.flag_unique {
                            break;
                        }
                    }
                    cmp::Ordering::Less => {
                        if self.flag_descended {
                            break;
                        }
                    }
                    cmp::Ordering::Greater => {
                        if self.flag_ascended {
                            break;
                        }
                    }
                }
            } else {
                break;
            }
            pos = next_pos;
        }
        if !self.flag_unique {
            self.rdr[flag_loop].seek(first_eq_pos.clone())?;
        } else if tmp_key != key.to_vec() {
            self.rdr[flag_loop].seek(pos.clone())?;
        }
        Ok(())
    }
}

impl OpArgs {
    fn new_io_state<T: IoRedef + ?Sized>(
        &self,
        ioobj: &T,
    ) -> CliResult<IoState<Box<dyn io::Write>>> {
        let mut rdr = Vec::new();
        let mut sel = Vec::new();
        let num = self.arg_input.len();
        let mut i = 0;
        let mut sel_num = 0;
        while i < num {
            let rconf1 = Config::new(&Some(self.arg_input[i].clone()), ioobj)
                .delimiter(self.flag_delimiter)
                .no_headers(self.flag_no_headers)
                .select(self.arg_column[i].clone());
            let mut rdr1 = rconf1.reader_file()?;
            let sel1 = self.get_selections(&rconf1, &mut rdr1)?;
            if sel_num == 0 {
                sel_num = sel1.len();
            } else if sel_num != sel1.len() {
                return Err(CliError::Other(
                    "Column selections must have the same number of columns,\
                     but found column selections with {} and {} columns."
                        .to_string(),
                ));
            }
            rdr.push(rdr1);
            sel.push(sel1);
            i += 1;
        }
        Ok(IoState {
            wtr: Config::new(&self.flag_output, ioobj).writer()?,
            rdr: rdr,
            sel: sel,
            no_headers: self.flag_no_headers,
            casei: self.flag_no_case,
            flag_unique: self.flag_unique,
            flag_ascended: self.flag_ascended,
            flag_descended: self.flag_descended,
            flag_numeric: self.flag_numeric,
            nulls: self.flag_nulls,
        })
    }

    fn get_selections<R: io::Read, T: IoRedef + ?Sized>(
        &self,
        rconf1: &Config<T>,
        rdr1: &mut csv::Reader<R>,
    ) -> CliResult<Selection> {
        let headers1 = rdr1.byte_headers()?;
        let select1 = rconf1.selection(&*headers1)?;
        Ok(select1)
    }
}
