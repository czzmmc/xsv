use std::cmp;
use std::prelude::v1::*;
use std::vec::Vec;
use workdir::Workdir;

fn prop_sort(name: &str, rows: Vec<Vec<String>>, headers: bool) -> bool {
    let wrk = Workdir::new("sort", name);
    wrk.create(&format!("{}_in.csv", name), rows.clone());
    let dir = wrk.result_dir();
    let outpath = format!("{}/test-result-{}", dir, name);
    let mut cmd = wrk.command("sort");
    cmd.push("-o");
    cmd.push(&outpath);
    let input = &format!("{}/{}_in.csv", dir, name);
    cmd.push(input);
    if !headers {
        cmd.push("--no-headers");
    }
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run(cmd) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]));
    } else {
        panic!("run error!");
    }
    let mut expected = rows.clone();
    let headers = if headers && !expected.is_empty() {
        expected.remove(0)
    } else {
        vec![]
    };
    expected.sort_by(|r1, r2| iter_cmp(r1.iter(), r2.iter()));
    if !headers.is_empty() {
        expected.insert(0, headers);
    }
    rassert_eq!(got, expected)
}

pub fn prop_sort_headers() {
    let rows = vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["LETTER", "b"],
        svec!["2", "c"],
        svec!["1", "d"],
    ];
    prop_sort("prop_sort_headers", rows, true);
}

pub fn prop_sort_no_headers() {
    let rows = vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["LETTER", "b"],
        svec!["2", "c"],
        svec!["1", "d"],
    ];
    prop_sort("prop_sort_no_headers", rows, false);
}

pub fn sort_select() {
    let wrk = Workdir::new("sort", "sort_select");
    wrk.create("sort_select_in.csv", vec![svec!["1", "b"], svec!["2", "a"]]);
    let dir = wrk.result_dir();
    let input = &format!("{}/sort_select_in.csv", dir);
    let outpath = format!("{}/test-result-sort_select", dir);
    let mut cmd = wrk.command("sort");
    cmd.push("-o");
    cmd.push(&outpath);
    cmd.push("--no-headers");
    cmd.extend(&["--select", "2"]);
    cmd.push(input);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run(cmd) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]));
    } else {
        panic!("run error!");
    }
    let expected = vec![svec!["2", "a"], svec!["1", "b"]];
    assert_eq!(got, expected);
}

pub fn sort_numeric() {
    let wrk = Workdir::new("sort", "sort_numeric");
    let dir = wrk.result_dir();
    let input = &format!("{}/sort_numeric_in.csv", dir);
    let outpath = format!("{}/test-result-sort_numeric", dir);

    wrk.create(
        "sort_numeric_in.csv",
        vec![
            svec!["N", "S"],
            svec!["10", "a"],
            svec!["LETTER", "b"],
            svec!["2", "c"],
            svec!["1", "d"],
        ],
    );

    let mut cmd = wrk.command("sort");
    cmd.push("-o");
    cmd.push(&outpath);
    cmd.push("-N");
    cmd.push(input);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run(cmd) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]));
    } else {
        panic!("run error!");
    }
    let expected = vec![
        svec!["N", "S"],
        //Non-numerics should be put first
        svec!["LETTER", "b"],
        svec!["1", "d"],
        svec!["2", "c"],
        svec!["10", "a"],
    ];
    assert_eq!(got, expected);
}

pub fn sort_numeric_non_natural() {
    let wrk = Workdir::new("sort", "sort_numeric_non_natural");
    let dir = wrk.result_dir();
    let input = &format!("{}/sort_numeric_non_natural_in.csv", dir);
    let outpath = format!("{}/test-result-sort_numeric_non_natural", dir);

    wrk.create(
        "sort_numeric_non_natural_in.csv",
        vec![
            svec!["N", "S"],
            svec!["8.33", "a"],
            svec!["5", "b"],
            svec!["LETTER", "c"],
            svec!["7.4", "d"],
            svec!["3.33", "e"],
        ],
    );

    let mut cmd = wrk.command("sort");
    cmd.push("-o");
    cmd.push(&outpath);
    cmd.push("-N");
    cmd.push(input);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run(cmd) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    let expected = vec![
        svec!["N", "S"],
        //Non-numerics should be put first
        svec!["LETTER", "c"],
        svec!["3.33", "e"],
        svec!["5", "b"],
        svec!["7.4", "d"],
        svec!["8.33", "a"],
    ];
    assert_eq!(got, expected);
}

pub fn sort_reverse() {
    let wrk = Workdir::new("sort", "sort_reverse");
    let dir = wrk.result_dir();
    let input = &format!("{}/sort_reverse_in.csv", dir);
    let outpath = format!("{}/test-result-sort_reverse", dir);

    wrk.create(
        "sort_reverse_in.csv",
        vec![svec!["R", "S"], svec!["1", "b"], svec!["2", "a"]],
    );

    let mut cmd = wrk.command("sort");
    cmd.push("-o");
    cmd.push(&outpath);
    cmd.push("-R");
    cmd.push("--no-headers");
    cmd.push(input);

    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run(cmd) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]));
    } else {
        panic!("run error!");
    }
    let expected = vec![svec!["R", "S"], svec!["2", "a"], svec!["1", "b"]];
    assert_eq!(got, expected);
}

/// Order `a` and `b` lexicographically using `Ord`
pub fn iter_cmp<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    A: Ord,
    L: Iterator<Item = A>,
    R: Iterator<Item = A>,
{
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}
