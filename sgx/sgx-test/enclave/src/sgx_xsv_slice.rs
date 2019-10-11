use std::borrow::ToOwned;
use std::prelude::v1::*;
use std::vec::Vec;
use workdir::Workdir;

macro_rules! slice_tests {
    ($name:ident, $start:expr, $end:expr, $expected:expr) => {
        pub mod $name {
            use super::test_slice;
            use std::prelude::v1::*;
            use std::vec::Vec;
            pub fn headers_no_index() {
                let name = concat!(stringify!($name), "headers_no_index");
                test_slice(name, $start, $end, $expected, true, false, false);
            }

            pub fn no_headers_no_index() {
                let name = concat!(stringify!($name), "no_headers_no_index");
                test_slice(name, $start, $end, $expected, false, false, false);
            }

            pub fn headers_index() {
                let name = concat!(stringify!($name), "headers_index");
                test_slice(name, $start, $end, $expected, true, true, false);
            }

            pub fn no_headers_index() {
                let name = concat!(stringify!($name), "no_headers_index");
                test_slice(name, $start, $end, $expected, false, true, false);
            }

            pub fn headers_no_index_len() {
                let name = concat!(stringify!($name), "headers_no_index_len");
                test_slice(name, $start, $end, $expected, true, false, true);
            }

            pub fn no_headers_no_index_len() {
                let name = concat!(stringify!($name), "no_headers_no_index_len");
                test_slice(name, $start, $end, $expected, false, false, true);
            }

            pub fn headers_index_len() {
                let name = concat!(stringify!($name), "headers_index_len");
                test_slice(name, $start, $end, $expected, true, true, true);
            }

            pub fn no_headers_index_len() {
                let name = concat!(stringify!($name), "no_headers_index_len");
                test_slice(name, $start, $end, $expected, false, true, true);
            }
        }
    };
}

fn setup(name: &str, headers: bool, use_index: bool) -> (Workdir, Vec<String>) {
    let wrk = Workdir::new("slice",name);
    let mut data = vec![svec!["a"], svec!["b"], svec!["c"], svec!["d"], svec!["e"]];
    if headers {
        data.insert(0, svec!["header"]);
    }

    let dir = wrk.result_dir();
    let input = format!("{}/{}_in.csv",dir,name);

    let outpath = format!("{}/test-result-{}",dir,name);

    if use_index {
        wrk.create_indexed(&format!("{}_in.csv",name), data);
    } else {
        wrk.create(&format!("{}_in.csv",name), data);
    }
    
    let mut cmd:Vec<String> = wrk.command("slice").into_iter().map(|s| s.to_string()).collect();;
    cmd.push(input);
    cmd.push("-o".to_string());
    cmd.push(outpath);
    (wrk, cmd)
}

fn test_slice(
    name: &str,
    start: Option<usize>,
    end: Option<usize>,
    expected: &[&str],
    headers: bool,
    use_index: bool,
    as_len: bool,
) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    if let Some(start) = start {
        cmd.push("--start".to_string());
        cmd.push(start.to_string());
    }
    if let Some(end) = end {
        if as_len {
            let start = start.unwrap_or(0);
            cmd.push("--len".to_string());
            cmd.push((end - start).to_string());
        } else {
            cmd.push("--end".to_string());
            cmd.push(end.to_string());
        }
    }
    if !headers {
        cmd.push("--no-headers".to_string());
    }
    
    let mut got: Vec<Vec<String>>=Vec::new();
    if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let mut expected = expected
        .iter()
        .map(|&s| vec![s.to_owned()])
        .collect::<Vec<Vec<String>>>();
    if headers {
        expected.insert(0, svec!["header"]);
    }
    assert_eq!(got, expected);
}

fn test_index(name: &str, idx: usize, expected: &str, headers: bool, use_index: bool) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    cmd.push("--index".to_string());
    cmd.push(idx.to_string());
    if !headers {
        cmd.push("--no-headers".to_string());
    }

    let mut got: Vec<Vec<String>>=Vec::new();
    if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let mut expected = vec![vec![expected.to_owned()]];
    if headers {
        expected.insert(0, svec!["header"]);
    }
    assert_eq!(got, expected);
}

slice_tests!(slice_simple, Some(0), Some(1), &["a"]);
slice_tests!(slice_simple_2, Some(1), Some(3), &["b", "c"]);
slice_tests!(slice_no_start, None, Some(1), &["a"]);
slice_tests!(slice_no_end, Some(3), None, &["d", "e"]);
slice_tests!(slice_all, None, None, &["a", "b", "c", "d", "e"]);


pub fn slice_index() {
    test_index("slice_index", 1, "b", true, false);
}

pub fn slice_index_no_headers() {
    test_index("slice_index_no_headers", 1, "b", false, false);
}

pub fn slice_index_withindex() {
    test_index("slice_index_withindex", 1, "b", true, true);
}

pub fn slice_index_no_headers_withindex() {
    test_index("slice_index_no_headers_withindex", 1, "b", false, true);
}
