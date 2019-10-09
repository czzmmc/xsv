#![cfg_attr(all(feature = "mesalock_sgx", not(target_env = "sgx")), no_std)]
#![cfg_attr(
    all(target_env = "sgx", target_vendor = "mesalock"),
    feature(rustc_private)
)]

#[cfg(all(feature = "mesalock_sgx", not(target_env = "sgx")))]
#[macro_use()]
extern crate sgx_tstd as std;
use std::prelude::v1::*;
extern crate byteorder;
// extern crate crossbeam_channel as channel;
extern crate csv;
extern crate csv_index;
extern crate docopt;
// extern crate filetime;
// extern crate num_cpus;
extern crate rand;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate stats;
// extern crate tabwriter;
// extern crate threadpool;

use std::borrow::ToOwned;
use std::env;
use std::fmt;
use std::io;
use std::string::String;
// use std::process;
use docopt::{parse::Parser, Docopt};
use std::format;
use std::io::{Error, ErrorKind};
pub type rResult<T> = std::result::Result<T, Error>;
use std::boxed::Box;
#[cfg(not(feature = "mesalock_sgx"))]
use std::fs;
use std::path::{Path,PathBuf};
#[cfg(feature = "mesalock_sgx")]
use std::untrusted::fs;
use std::vec;
// trait io::Seek + io::Read: io::Seek + io::Read {}
// impl<T: io::Seek + io::Read> io::Seek + io::Read for T {}

macro_rules! wout {
    ($($arg:tt)*) => ({
        use std::fmt::Write;
        let mut errmsg = String::new();
        (writeln!(&mut errmsg, $($arg)*)).unwrap();
        errmsg
    })
}

macro_rules! werr {
    ($($arg:tt)*) => ({
        use std::io::Write;
        (writeln!(&mut ::std::io::stderr(), $($arg)*)).unwrap();
    });
}

macro_rules! fail {
    ($e:expr) => {
        Err(::std::convert::From::from($e))
    };
}

macro_rules! command_list {
    () => {
        "
    cat         Concatenate by row or column
    count       Count records
    fixlengths  Makes all records have same length
    flatten     Show one field per line
    fmt         Format CSV output (change field delimiter)
    frequency   Show frequency tables
    headers     Show header names
    help        Show this usage message.
    index       Create CSV index for faster access
    input       Read CSV data with special quoting rules
    join        Join CSV files
    partition   Partition CSV data based on a column value
    sample      Randomly sample CSV data
    reverse     Reverse rows of CSV data
    search      Search CSV data with regexes
    select      Select columns from CSV
    slice       Slice records from CSV
    sort        Sort CSV data
    split       Split CSV data into many files
    stats       Compute basic statistics
    table       Align CSV data into columns
"
    };
}
use std::io::{Read,Write,IoSlice};
#[derive(Clone)]
pub struct CommonXsv
{   reader:io::Cursor<Vec<u8>>,
    writer:io::Cursor<Vec<u8>>,
    path:Option<String>,

}
impl CommonXsv {
    pub fn new()->CommonXsv{
        CommonXsv{
            reader:io::Cursor::new(vec![]),
            writer:io::Cursor::new(vec![]),
            path:None,
        }
    }
}
impl Write for CommonXsv {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
       self.writer.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.writer.write_vectored(bufs)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> { 
        // let mm =String::from_utf8(self.writer.clone().into_inner());
        match &self.path{
            None=> panic!("not outpath"),
            Some(p)=>{
                let mut f =fs::File::create(p)?;
                f.write_all(&self.writer.clone().into_inner())?;
                f.flush()?;
            }
        }
        Ok(())
        }
}
impl Ioredef for CommonXsv{
    fn io_reader(&mut self, path: Option<PathBuf>) -> io::Result<Box<io::Read>> {
                    Ok(match path {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Invalid input file",
                ))
            }
            Some(ref p) => match fs::File::open(p) {
                Ok(mut x) => {
                        let mut tmp = String::new();
                        x.read_to_string(&mut tmp)?;
                        self.reader = io::Cursor::new(tmp.as_bytes().to_owned());
                        Box::new(self.reader.to_owned())},
                Err(err) => {
                    let msg = format!("failed to open {}", err);
                    return Err(io::Error::new(io::ErrorKind::NotFound, msg));
                }
            },
        })
    }
    fn io_writer(&mut self, path: Option<PathBuf>) -> io::Result<Box<io::Write>> {
         Ok(Box::new(CommonXsv{
            reader:io::Cursor::new(vec![]),
            writer:io::Cursor::new(vec![]),
            path: Some(path.unwrap().to_str().unwrap().to_string()),
            }))
            
    }
    fn read_from_file(&mut self, path: Option<PathBuf>) -> io::Result<Box<dyn SeekRead>> {
        Ok( match path{
            None => return Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot use read file here",
            )),
            Some(p) =>Box::new(fs::File::open(p)?),
        }
            )
            
        
    }
    
}
trait SeekRead: io::Seek + io::Read {}
impl<T: io::Seek + io::Read> SeekRead for T {}

pub trait Ioredef {
    fn io_reader(&mut self, path: Option<PathBuf>) -> io::Result<Box<io::Read>> {
        Ok(match path {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Invalid input file",
                ))
            }
            Some(ref p) => match fs::File::open(p) {
                Ok(x) => Box::new(x),
                Err(err) => {
                    let msg = format!("failed to open {}", err);
                    return Err(io::Error::new(io::ErrorKind::NotFound, msg));
                }
            },
        })
    }
    fn io_writer(&mut self, path: Option<PathBuf>) -> io::Result<Box<io::Write>> {
        Ok(match path {
            None => Box::new(vec![]),
            Some(ref p) => Box::new(fs::File::create(p)?),
        })
    }
    fn read_from_file(&mut self, path: Option<PathBuf>) -> io::Result<Box<dyn SeekRead>> {
        Ok(match path {
            None => Box::new(io::Cursor::new(vec![])),
            Some(ref p) => Box::new(fs::File::open(p)?),
        })
    }
}
mod cmd;
mod config;
mod index;
mod select;
mod util;

static USAGE: &'static str = concat!(
    "
Usage:
    xsv <command> [<args>...]
    xsv [options]

Options:
    --list        List all commands available.
    -h, --help    Display this message
    <command> -h  Display the command help message
    --version     Print version info and exit

Commands:",
    command_list!()
);
use serde::Deserialize;
#[derive(Deserialize)]
struct Args {
    arg_command: Option<Command>,
    flag_list: bool,
}
pub struct XsvMain;
impl XsvMain {
    pub fn new<T: Ioredef + Clone>(arg: Vec<&str>, ioobj: T) -> rResult<()> {
        let args: Args = Docopt::new(USAGE)
            .and_then(|d| {
                d.argv(arg.clone())
                    .options_first(true)
                    .version(Some(util::version()))
                    .deserialize()
            })
            .map_err(|_| Error::from(ErrorKind::InvalidData))?;

        if args.flag_list {
            let errmsg = wout!(concat!("Installed commands:", command_list!()));
            panic!("{}",errmsg);
            return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
        }
        match args.arg_command {
            None => {
                werr!(concat!(
                    "xsv is a suite of CSV command line utilities.

Please choose one of the following commands:",
                    command_list!()
                ));
                return Ok(());
            }
            Some(cmd) => {
                match cmd.run(
                    arg.clone().into_iter().map(|s| s.to_owned()).collect(),
                    ioobj,
                ) {
                    Ok(()) => return Ok(()),
                    Err(CliError::Flag(err)) => {
                        let errmsg = wout!("{}", err);
                        return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
                    },
                    Err(CliError::Csv(err)) => {
                        let errmsg = wout!("{}", err);
                        return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
                    }
                    Err(CliError::Io(ref err)) if err.kind() == io::ErrorKind::BrokenPipe => {
                        let errmsg = wout!("{}", err);
                        return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
                    }
                    Err(CliError::Io(err)) => {
                        let errmsg = wout!("{}", err);
                        return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
                    }
                    Err(CliError::Other(msg)) => {
                       let errmsg = wout!("{}", msg);
                        return Err(Error::new(
                        ErrorKind::InvalidData,
                        errmsg,
                    ));
                    }
                };
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Command {
    Cat,
    Count,
    FixLengths,
    Flatten,
    Fmt,
    Frequency,
    Headers,
    Help,
    Index,
    Input,
    Join,
    Partition,
    Reverse,
    Sample,
    Search,
    Select,
    Slice,
    Sort,
    Split,
    Stats,
    Table,
}

impl Command {
    fn run<T: Ioredef + Clone>(self, arg: Vec<String>, ioobj: T) -> CliResult<()> {
        //let argv: Vec<_> = env::args().map(|v| v.to_owned()).collect();
        let argv: Vec<_> = arg.iter().map(|s| &**s).collect();
        //panic!("{:?}",argv);
        let argv = &*argv;

        if !argv[1].chars().all(char::is_lowercase) {
            return Err(CliError::Other(
                format!(
                    "xsv expects commands in lowercase. Did you mean '{}'?",
                    argv[1].to_lowercase()
                )
                .to_string(),
            ));
        }
        match self {
            Command::Cat => cmd::cat::run(argv, ioobj),
            Command::Count => cmd::count::run(argv, ioobj),
            Command::FixLengths => cmd::fixlengths::run(argv, ioobj),
            Command::Flatten => cmd::flatten::run(argv, ioobj),
            Command::Fmt => cmd::fmt::run(argv, ioobj),
            Command::Frequency => cmd::frequency::run(argv, ioobj),
            Command::Headers => cmd::headers::run(argv, ioobj),
            Command::Help => {
                wout!("{}", USAGE);
                Ok(())
            }
            Command::Index => cmd::index::run(argv, ioobj),
            Command::Input => cmd::input::run(argv, ioobj),
            Command::Join => cmd::join::run(argv, ioobj),
            Command::Partition => cmd::partition::run(argv, ioobj),
            Command::Reverse => cmd::reverse::run(argv, ioobj),
            Command::Sample => cmd::sample::run(argv, ioobj),
            Command::Search => cmd::search::run(argv, ioobj),
            Command::Select => cmd::select::run(argv, ioobj),
            Command::Slice => cmd::slice::run(argv, ioobj),
            Command::Sort => cmd::sort::run(argv, ioobj),
            Command::Split => cmd::split::run(argv, ioobj),
            Command::Stats => cmd::stats::run(argv, ioobj),
            Command::Table => cmd::table::run(argv, ioobj),
        }
    }
}

pub type CliResult<T> = Result<T, CliError>;

#[derive(Debug)]
pub enum CliError {
    Flag(docopt::Error),
    Csv(csv::Error),
    Io(io::Error),
    Other(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CliError::Flag(ref e) => e.fmt(f),
            CliError::Csv(ref e) => e.fmt(f),
            CliError::Io(ref e) => e.fmt(f),
            CliError::Other(ref s) => f.write_str(&**s),
        }
    }
}

impl From<docopt::Error> for CliError {
    fn from(err: docopt::Error) -> CliError {
        CliError::Flag(err)
    }
}

impl From<csv::Error> for CliError {
    fn from(err: csv::Error) -> CliError {
        if !err.is_io_error() {
            return CliError::Csv(err);
        }
        match err.into_kind() {
            csv::ErrorKind::Io(v) => From::from(v),
            _ => unreachable!(),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> CliError {
        CliError::Io(err)
    }
}

impl From<String> for CliError {
    fn from(err: String) -> CliError {
        CliError::Other(err)
    }
}

impl<'a> From<&'a str> for CliError {
    fn from(err: &'a str) -> CliError {
        CliError::Other(err.to_owned())
    }
}

impl From<regex::Error> for CliError {
    fn from(err: regex::Error) -> CliError {
        CliError::Other(format!("{:?}", err))
    }
}
