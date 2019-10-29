use std::prelude::v1::*;
use std::vec::Vec;
use workdir::Workdir;

/// This tests whether `xsv count` gets the right answer.
///
/// It does some simple case analysis to handle whether we want to test counts
/// in the presence of headers and/or indexes.
// fn prop_count_len(name: &str, rows: CsvData,
//                   headers: bool, idx: bool) -> bool {

//     let wrk = Workdir::new(name);
//     if idx {
//         wrk.create_indexed("in.csv", rows);
//     } else {
//         wrk.create("in.csv", rows);
//     }

//     let mut cmd = wrk.command("count");
//     if !headers {
//         cmd.arg("--no-headers");
//     }
//     cmd.arg("in.csv");

//     let got_count: usize = wrk.stdout(&mut cmd);
//     rassert_eq!(got_count, expected_count)
// }

fn mofigy_cmd<'a>(mut cmd: Vec<&'a str>, arg: &Vec<&'a str>) -> Vec<&'a str> {
    cmd.extend(arg);
    cmd
}
fn run_count(
    test_name: &str,
    fun_name: &str,
    rows: Vec<Vec<String>>,
    header: bool,
    idx: bool,
) -> Vec<Vec<String>> {
    let wrk = Workdir::new(test_name, fun_name);
    if idx {
        wrk.create_indexed(&format!("{}_in1.csv", fun_name), rows);
    } else {
        wrk.create(&format!("{}_in1.csv", fun_name), rows);
    }

    // wrk.create(&format!("{}_in1.csv",fun_name), rows1);

    let cmd = wrk.command("count");
    let dir = wrk.result_dir();
    let path1 = &format!("{}/{}_in1.csv", dir, fun_name);

    let outpath = &format!("{}/test-result-{}", dir, fun_name);
    let mut cmd = mofigy_cmd(cmd, &vec![&path1, "-o", &outpath]);
    if !header {
        cmd.push("--no-headers")
    }

    if wrk.run(cmd) {
        wrk.read_from_file(false).unwrap_or(vec![vec![]])
    } else {
        panic!("run error!");
    }
}

pub fn prop_count() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let mut expected_count = rows1.len();
    let headers = false;
    if headers && expected_count > 0 {
        expected_count -= 1;
    }
    let got: Vec<Vec<String>> = run_count("count", "prop_count", rows1, false, false);
    assert_eq!(got, vec![vec![expected_count.to_string()]]);
}
pub fn prop_count_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let mut expected_count = rows1.len();
    let headers = true;
    if headers && expected_count > 0 {
        expected_count -= 1;
    }
    let got: Vec<Vec<String>> = run_count("count", "prop_count_headers", rows1, headers, false);
    assert_eq!(got, vec![vec![expected_count.to_string()]]);
}
pub fn prop_count_indexed() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let mut expected_count = rows1.len();
    let headers = false;
    if headers && expected_count > 0 {
        expected_count -= 1;
    }
    let got: Vec<Vec<String>> = run_count("count", "prop_count_indexed", rows1, headers, true);
    assert_eq!(got, vec![vec![expected_count.to_string()]]);
}

pub fn prop_count_indexed_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let mut expected_count = rows1.len();
    let headers = true;
    if headers && expected_count > 0 {
        expected_count -= 1;
    }
    let got: Vec<Vec<String>> =
        run_count("count", "prop_count_indexed_headers", rows1, headers, true);
    assert_eq!(got, vec![vec![expected_count.to_string()]]);
}
