use csv;
use std::fmt;
use std::io::{self, Read};
use std::mem::transmute;
use std::path::{Path, PathBuf};
use std::prelude::v1::*;
use std::str::FromStr;
use std::sync::atomic;
use std::untrusted::fs;
use uuid::Uuid;
use xsv::{IoRedef, SeekRead, XsvMain};

static XSV_INTEGRATION_TEST_DIR: &'static str = "xit";

static NEXT_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub struct Workdir {
    root: PathBuf,
    dir: PathBuf,
    flexible: bool,
    outpath: Option<PathBuf>,
}

#[derive(Clone)]
pub struct CommonXsv;
impl IoRedef for CommonXsv {
    fn io_reader(&self, path: Option<PathBuf>) -> io::Result<Box<dyn io::Read>> {
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
    fn io_writer(&self, path: Option<PathBuf>) -> io::Result<Box<dyn io::Write>> {
        Ok(match path {
            None => Box::new(vec![]),
            Some(ref p) => Box::new(fs::File::create(p)?),
        })
    }
    fn read_from_file(&self, path: Option<PathBuf>) -> io::Result<Box<dyn SeekRead>> {
        Ok(match path {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Cannot use read file here",
                ))
            }
            Some(p) => Box::new(fs::File::open(p)?),
        })
    }
    fn io_tmp_writer(&self, path: Option<PathBuf>) -> io::Result<(String, Box<dyn io::Write>)> {
        Ok(match path {
            None => {
                let file_id = Uuid::new_v4().to_string();
                (file_id.clone(), Box::new(fs::File::create(file_id)?))
            }
            Some(ref p) => (
                p.to_str().unwrap_or("").to_string(),
                Box::new(fs::File::create(p)?),
            ),
        })
    }
    fn remove_tmp_file(&self, path: String) -> io::Result<()> {
        fs::remove_file(path)?;
        Ok(())
    }
}
pub fn simple_test_join() {
    let tt = CommonXsv;
    assert!(XsvMain::new(
        vec![
            "xsv",
            "join",
            "MD5",
            "mydb.csv",
            "MD5",
            "mydb.csv",
            "--no-case",
            "-o",
            "test_data_join2.csv"
        ],
        &tt
    )
    .is_ok())
}
pub fn simple_test_split() {
    let tt = CommonXsv;
    assert!(XsvMain::new(vec!["xsv", "split", "", "mydb.csv",], &tt).is_ok())
}
pub fn simple_test_headers() {
    let tt = CommonXsv;
    assert!(XsvMain::new(
        vec![
            "xsv",
            "headers",
            "-o",
            "test_headers",
            "--intersect",
            "mydb.csv",
            "mydb2.csv"
        ],
        &tt
    )
    .is_ok())
}
pub fn simple_test_partition() {
    let tt = CommonXsv;
    assert!(XsvMain::new(vec!["xsv", "partition", "1", "", "mydb.csv",], &tt).is_ok())
}
pub fn test_multijoin() {
    let tt = CommonXsv;
    assert!(XsvMain::new(
        vec![
            "xsv",
            "multijoin",
            "--no-case",
            "--ascended",
            "-N",
            "-o",
            "my_mutil_num_sorted_sample.txt",
            "1",
            "n_sorted_sample.txt",
            "1",
            "n_sorted_sample.txt",
        ],
        &tt
    )
    .is_ok())
}

impl Workdir {
    pub fn new(name: &str, funname: &str) -> Workdir {
        let mut root = PathBuf::from("/tmp");
        if root.ends_with("deps") {
            root.pop();
        }
        let dir = root.join(XSV_INTEGRATION_TEST_DIR).join(name);
        Workdir {
            root: root,
            dir: dir.clone(),
            flexible: false,
            outpath: Some(dir.join(&format!("test-result-{}", funname))),
        }
    }

    pub fn flexible(mut self, yes: bool) -> Workdir {
        self.flexible = yes;
        self
    }

    pub fn create(&self, name: &str, rows: Vec<Vec<String>>) {
        let mut wtr = csv::WriterBuilder::new()
            .flexible(self.flexible)
            .from_writer(fs::File::create(self.dir.join(name)).unwrap());

        for row in rows.into_iter() {
            wtr.write_record(row).unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn create_indexed(&self, name: &str, rows: Vec<Vec<String>>) {
        self.create(name, rows);

        let mut cmd = self.command("index");
        let dir = &format!(
            "{}/{}",
            self.dir.clone().into_os_string().into_string().unwrap(),
            name
        );
        cmd.push(dir);
        self.run(cmd);
    }

    pub fn reader_file(&self, header: bool) -> io::Result<csv::Reader<fs::File>> {
        match self.outpath {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot use <stdin> here",
            )),
            Some(ref p) => fs::File::open(p)
                .map(|f| csv::ReaderBuilder::new().has_headers(header).from_reader(f)),
        }
    }
    pub fn read_from_file(&self, header: bool) -> io::Result<Vec<Vec<String>>> {
        let mut rdr = self.reader_file(header)?;
        let records: Vec<Vec<String>> = rdr
            .records()
            .collect::<Result<Vec<csv::StringRecord>, _>>()
            .unwrap()
            .into_iter()
            .map(|r| r.iter().map(|f| f.to_string()).collect())
            .collect();
        Ok(unsafe { transmute(records) })
    }
    pub fn command<'a>(&self, sub_command: &'a str) -> Vec<&'a str> {
        let mut cmd = vec!["xsv"];
        cmd.push(sub_command);
        cmd
    }

    pub fn run(&self, cmd: Vec<&str>) -> bool {
        let tt = CommonXsv;
        (XsvMain::new(cmd, &tt)).is_ok()
    }

    pub fn assert_err(&self, cmd: Vec<&str>) {
        let o = self.run(cmd.to_owned());
        if o {
            panic!(
                "\n\n===== {:?} =====\n\
                 command succeeded but expected failure!\
                 \n\ncwd: {}\
                 \n\nstatus: {}\
                 \n\n=====\n",
                cmd,
                self.dir.display(),
                o
            );
        }
    }

    pub fn from_str<T: FromStr>(&self, name: &Path) -> T {
        let mut o = String::new();
        fs::File::open(name)
            .unwrap()
            .read_to_string(&mut o)
            .unwrap();
        o.parse().ok().expect("fromstr")
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    pub fn result_dir(&self) -> &str {
        self.dir.to_str().unwrap()
    }
}

impl fmt::Debug for Workdir {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "path={}", self.dir.display())
    }
}
