use crate::cmd::common::{check_num_type, cmp_key, get_row_key};
use byteorder::{ByteOrder, LittleEndian};
use config::{Config, Delimiter};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use select::{SelectColumns, Selection};
use std::prelude::v1::*;
use std::{cmp, io};
use util;
use {CliError, CliResult};

static USAGE: &'static str = "
Sorts CSV data lexicographically.

Usage:
    xsv terasort [options] [<input>]

sort options:
    -s, --select <arg>     Select a subset of columns to sort.
                           See 'xsv select --help' for the format details.
                           [default: 1]
    -N, --numeric          Compare according to string numerical value
    -R, --reverse          Reverse order
    --seed <number>        RNG seed.
    --samplesize <number>  the size of the sample.[default: 5000]
    --splitnum <number>    the size of reduce.[default: 10]
    --no-case              When set, sort are done case insensitively.
    --nulls                When set, sort will work on empty fields.
                           Otherwise, empty fields are completely ignored.
                           (In fact, any row that has an empty field in the
                           key specified is ignored.)
Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be sorted with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_numeric: bool,
    flag_reverse: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_seed: Option<usize>,
    flag_samplesize: usize,
    flag_splitnum: usize,
    flag_no_case: bool,
    flag_nulls: bool,
}
use IoRedef;
pub fn run<T: IoRedef + ?Sized>(argv: &[&str], ioobj: &T) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let numeric = args.flag_numeric;
    if args.flag_splitnum == 0 || args.flag_samplesize == 0 {
        return Err(CliError::Other("parameters error.".to_string()));
    }
    let sample = args.get_sampled(ioobj)?;
    let mut points = args.get_split_point(sample)?;
    let splits = args.split_file_by_key(&mut points, ioobj)?;
    let rconf = Config::new(&args.flag_output, ioobj);
    let mut wtr = rconf.writer()?;
    let mut written_header = false;
    let count = splits.len();
    for r in 0..count {
        let input = if args.flag_reverse {
            splits[count - 1 - r].clone()
        } else {
            splits[r].clone()
        };
        let mut sort_file: SortKey<Box<dyn std::io::Write>> = SortKey {
            arg_input: Some(input.clone()),
            flag_select: args.flag_select.clone(),
            flag_numeric: numeric,
            wtr: &mut wtr,
            flag_no_headers: args.flag_no_headers,
            flag_delimiter: args.flag_delimiter,
            flag_reverse: args.flag_reverse,
            flag_no_case: args.flag_no_case,
            flag_nulls: args.flag_nulls,
        };
        sort_file.sort_by_key(ioobj, &mut written_header)?;
        let _ = rconf.remove_tmp_file(input);
    }
    wtr.flush()?;

    Ok(())
}
type ByteString = Vec<u8>;
impl Args {
    fn get_sampled<T: IoRedef + ?Sized>(&self, ioobj: &T) -> CliResult<Vec<Vec<ByteString>>> {
        let rconfig = Config::new(&self.arg_input, ioobj)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.flag_select.clone());
        let mut rdr = rconfig.reader()?;
        let headers = rdr.byte_headers()?.clone();
        let sel = rconfig.selection(&headers)?;
        self.sample_reservoir(&mut rdr, sel)
    }

    fn sample_reservoir<R: std::io::Read>(
        &self,
        rdr: &mut csv::Reader<R>,
        sel: Selection,
    ) -> CliResult<Vec<Vec<ByteString>>> {
        // The following algorithm has been adapted from:
        // https://en.wikipedia.org/wiki/Reservoir_sampling
        let mut reservoir = Vec::with_capacity(self.flag_samplesize as usize);
        let mut records = rdr.byte_records().enumerate();

        for (_, row) in records.by_ref().take(reservoir.capacity()) {
            let row = row?;
            let key = get_row_key(&sel, &row, self.flag_no_case);
            if self.flag_numeric {
                check_num_type(&key)?;
            }
            reservoir.push(key);
        }
        // Seeding rng
        let mut rng: StdRng = match self.flag_seed {
            None => StdRng::from_rng(rand::thread_rng()).unwrap(),
            Some(seed) => {
                let mut buf = [0u8; 32];
                LittleEndian::write_u64(&mut buf, seed as u64);
                SeedableRng::from_seed(buf)
            }
        };
        // Now do the sampling.
        for (i, row) in records {
            let row = row?;
            let key = get_row_key(&sel, &row, false);
            let random = rng.gen_range(0, i + 1);
            if random < self.flag_samplesize as usize {
                if self.flag_numeric {
                    check_num_type(&key)?;
                }
                reservoir[random] = key;
            }
        }
        Ok(reservoir)
    }
    fn get_split_point(&self, mut sample: Vec<Vec<ByteString>>) -> CliResult<Vec<Vec<ByteString>>> {
        sample.sort_by(|k1, k2| cmp_key(k1, k2, self.flag_numeric));
        sample.dedup_by(|k1, k2| k1 == k2);

        let sample_size = sample.len();
        let splitnum = if self.flag_splitnum > sample_size {
            sample_size
        } else {
            self.flag_splitnum
        };

        let mut point: Vec<Vec<ByteString>> = Vec::new();
        let p = sample_size / splitnum;
        let q = (sample_size % splitnum) / 2;
        let mut m = 0;
        for i in 1..splitnum {
            if m < q {
                point.push(sample[p * i + m].clone());
                m += 1;
            } else {
                point.push(sample[p * i + m].clone());
            }
        }
        Ok(point)
    }

    fn get_split_writers<T: IoRedef + ?Sized>(
        &self,
        points: &[Vec<ByteString>],
        ioobj: &T,
    ) -> CliResult<Vec<(String, csv::Writer<Box<dyn io::Write>>)>> {
        let mut hashmap: Vec<(String, csv::Writer<Box<dyn io::Write>>)> =
            Vec::with_capacity(points.len() + 1);
        for _i in 0..=points.len() {
            hashmap.push(
                Config::new(&None, ioobj)
                    .tmp_writer()
                    .map_err(|_| CliError::Other("parameters error.".to_string()))?,
            );
        }
        Ok(hashmap)
    }
    fn split_file_by_key<T: IoRedef + ?Sized>(
        &self,
        points: &mut Vec<Vec<ByteString>>,
        ioobj: &T,
    ) -> CliResult<Vec<String>> {
        let mut wtrs = self.get_split_writers(&points, ioobj)?;
        let rconfig = Config::new(&self.arg_input, ioobj)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.flag_select.clone());
        let mut rdr = rconfig.reader()?;
        let headers = rdr.byte_headers()?.clone();
        let sel = rconfig.selection(&headers)?;

        let wtr_len = wtrs.len();
        if !self.flag_no_headers && !headers.is_empty() {
            for wtr in wtrs.iter_mut().take(wtr_len) {
                wtr.1.write_record(&headers)?
            }
        }
        if wtr_len != points.len() + 1 {
            return Err(CliError::Other("tmp file error.".to_string()));
        }
        for row in rdr.byte_records() {
            let row = row?;
            let key = get_row_key(&sel, &row, self.flag_no_case);
            // panic!("sel {:?},key {:?},points {:?}",sel,key,points);
            let mut flag_write = false;
            for i in 0..points.len() {
                if cmp_key(&key, &points[i], self.flag_numeric) == cmp::Ordering::Greater {
                    continue;
                }
                wtrs[i].1.write_byte_record(&row)?;
                flag_write = true;
                break;
            }
            if !flag_write {
                wtrs[wtr_len - 1].1.write_byte_record(&row)?;
            };
        }
        let mut result_files: Vec<String> = Vec::new();
        for wtr in wtrs.iter_mut().take(wtr_len) {
            // for i in 0..wtr_len {
            wtr.1.flush()?;
            result_files.push(wtr.0.clone());
        }
        Ok(result_files)
    }
}

#[derive(Debug)]
struct SortKey<'a, W: std::io::Write> {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_numeric: bool,
    wtr: &'a mut csv::Writer<W>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_reverse: bool,
    flag_no_case: bool,
    flag_nulls: bool,
}
impl<'a, W: std::io::Write> SortKey<'a, W> {
    fn sort_by_key<T: IoRedef + ?Sized>(
        &mut self,
        ioobj: &T,
        written_header: &mut bool,
    ) -> CliResult<()> {
        let rconfig = Config::new(&self.arg_input, ioobj)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.flag_select.clone());
        let mut rdr = rconfig.reader_file()?;
        let headers = rdr.byte_headers()?.clone();
        if rdr.is_done() {
            return Ok(());
        }
        let sel = rconfig.selection(&headers)?;
        let mut validx = SortValueIndex::new(
            rdr,
            &sel,
            self.flag_no_case,
            self.flag_nulls,
            self.flag_reverse,
            self.flag_numeric,
        )?;

        if !rconfig.no_headers && !*written_header {
            self.wtr.write_record(&headers)?;
            *written_header = true;
        }
        let mut scratch = csv::ByteRecord::new();
        for pos in validx.values {
            validx
                .rdr
                .seek(csv::Position::new().set_byte(pos).clone())?;
            validx.rdr.read_byte_record(&mut scratch)?;
            self.wtr.write_byte_record(&scratch)?;
        }

        self.wtr.flush()?;
        Ok(())
    }
}

pub struct SortValueIndex<R> {
    // This maps tuples of values to corresponding rows.
    pub values: Vec<u64>,
    rdr: csv::Reader<R>,
}

impl<R: io::Read + io::Seek> SortValueIndex<R> {
    pub fn new(
        rdr: csv::Reader<R>,
        sel: &Selection,
        casei: bool,
        nulls: bool,
        reverse: bool,
        numeric: bool,
    ) -> CliResult<SortValueIndex<R>> {
        if numeric {
            sort_by_numeric(rdr, sel, casei, nulls, reverse)
        } else {
            sort_by_string(rdr, sel, casei, nulls, reverse)
        }
    }
}

macro_rules! sort_by {
    ($fname:ident,$trans:expr,$ty:ty,$cmp:expr) => {
        pub fn $fname<R: io::Read + io::Seek>(
            mut rdr: csv::Reader<R>,
            sel: &Selection,
            casei: bool,
            nulls: bool,
            reverse: bool,
        ) -> CliResult<SortValueIndex<R>> {
            let mut val_idx: Vec<($ty, u64)> = Vec::new();
            if !rdr.has_headers() {
                let mut pos = csv::Position::new();
                pos.set_byte(0);
                rdr.seek(pos)?;
            } else {
                rdr.byte_headers()?;
            }

            while let Some(row) = rdr.byte_records().next() {
                let row = row?;
                let fields: Vec<_> = sel.select(&row).map(|v| v).collect();
                if nulls || !fields.iter().any(|f| f.is_empty()) {
                    let tmp = $trans(fields[0], casei)?;
                    val_idx.push((tmp, row.position().unwrap().byte()));
                }
            }

            // quick_sort(&mut val_idx, reverse);
            if reverse {
                val_idx.sort_by(|r1, r2| $cmp(r2.clone(), r1.clone()));
            } else {
                val_idx.sort_by(|r1, r2| $cmp(r1.clone(), r2.clone()));
            }
            let mut sorted_pos = Vec::with_capacity(val_idx.len());
            for (_, pos) in val_idx.iter() {
                sorted_pos.push(pos.to_owned());
            }
            Ok(SortValueIndex {
                values: sorted_pos,
                rdr: rdr,
            })
        }
    };
}
sort_by!(sort_by_string, transform_string, String, compare_string);
sort_by!(sort_by_numeric, transform_numeric, f64, compare_float);

fn compare_float(f1: (f64, u64), f2: (f64, u64)) -> cmp::Ordering {
    f1.partial_cmp(&f2).unwrap_or(cmp::Ordering::Equal)
}
fn compare_string(s1: (String, u64), s2: (String, u64)) -> cmp::Ordering {
    s1.cmp(&s2)
}
pub fn quick_sort<T: PartialOrd>(s: &mut [(T, u64)], reverse: bool) {
    if s.len() > 1 {
        let pivot = compare_partition(s, reverse);
        quick_sort(&mut s[..pivot], reverse);
        quick_sort(&mut s[pivot + 1..], reverse);
    }
}
pub fn compare_partition<T: PartialOrd>(s: &mut [(T, u64)], reverse: bool) -> usize {
    let pivot = s.len() - 1;
    let mut swap = 0;
    for i in 0..pivot {
        if reverse {
            if s[i].0 < s[pivot].0 {
                if swap != i {
                    s.swap(swap, i);
                }
                swap += 1;
            }
        } else {
            if s[i].0 > s[pivot].0 {
                if swap != i {
                    s.swap(swap, i);
                }
                swap += 1;
            }
        }
    }
    if swap != pivot {
        s.swap(swap, pivot);
    }
    swap
}

pub fn transform_string(bs: &[u8], casei: bool) -> CliResult<String> {
    match std::str::from_utf8(bs) {
        Err(_) => Err(CliError::Other(
            "parse error.Filed's type is not utf8".to_string(),
        )),
        Ok(s) => {
            if !casei {
                Ok(s.trim().to_string())
            } else {
                let norm: String = s
                    .trim()
                    .chars()
                    .map(|c| c.to_lowercase().next().unwrap())
                    .collect();
                Ok(norm)
            }
        }
    }
}
pub fn transform_numeric(bs: &[u8], _casei: bool) -> CliResult<f64> {
    match std::str::from_utf8(bs) {
        Err(_) => Err(CliError::Other(
            "Parse error.Filed's type is not utf8".to_string(),
        )),
        Ok(s) => {
            let par = s.parse::<f64>().map_err(|_| {
                CliError::Other("Parse error.Filed's type is not numeric".to_string())
            })?;
            Ok(par)
        }
    }
}
