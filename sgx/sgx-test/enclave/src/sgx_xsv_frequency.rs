use csv;
use serde::Deserialize;
use stats::Frequencies;
use std::borrow::ToOwned;
use std::collections::hash_map::{Entry, HashMap};
use std::prelude::v1::*;
use std::vec;
use workdir::Workdir;

fn setup(name: &str) -> (Workdir, Vec<String>) {
    let rows = vec![
        svec!["h1", "h2"],
        svec!["a", "z"],
        svec!["a", "y"],
        svec!["a", "y"],
        svec!["b", "z"],
        svec!["", "z"],
        svec!["(NULL)", "x"],
    ];

    let wrk = Workdir::new("frequency", name);
    wrk.create(&format!("{}_in.csv", name), rows);
    let mut cmd: Vec<String> = wrk
        .command("frequency")
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let dir = wrk.result_dir();
    let input = format!("{}/{}_in.csv", dir, name);
    let outpath = format!("{}/test-result-{}", dir, name);
    cmd.push(input);
    cmd.push("-o".to_string());
    cmd.push(outpath);
    (wrk, cmd)
}

pub fn frequency_no_headers() {
    let (wrk, mut cmd) = setup("frequency_no_headers");
    cmd.extend(svec!["--limit", "0", "--select", "1", "--no-headers"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got = got.into_iter().skip(1).collect();
    got.sort();
    let expected = vec![
        svec!["1", "(NULL)", "1"],
        svec!["1", "(NULL)", "1"],
        svec!["1", "a", "3"],
        svec!["1", "b", "1"],
        svec!["1", "h1", "1"],
    ];
    assert_eq!(got, expected);
}

pub fn frequency_no_nulls() {
    let (wrk, mut cmd) = setup("frequency_no_nulls");
    cmd.extend(svec!["--no-nulls", "--limit", "0", "--select", "h1"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "a", "3"],
        svec!["h1", "b", "1"],
    ];
    assert_eq!(got, expected);
}

pub fn frequency_nulls() {
    let (wrk, mut cmd) = setup("frequency_nulls");
    cmd.extend(svec!["--limit", "0", "--select", "h1"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "a", "3"],
        svec!["h1", "b", "1"],
    ];
    assert_eq!(got, expected);
}

pub fn frequency_limit() {
    let (wrk, mut cmd) = setup("frequency_limit");
    cmd.extend(svec!["--limit", "1"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "a", "3"],
        svec!["h2", "z", "3"],
    ];
    assert_eq!(got, expected);
}

pub fn frequency_asc() {
    let (wrk, mut cmd) = setup("frequency_asc");
    cmd.extend(svec!["--limit", "1", "--select", "h2", "--asc"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got.sort();
    let expected = vec![svec!["field", "value", "count"], svec!["h2", "x", "1"]];
    assert_eq!(got, expected);
}

pub fn frequency_select() {
    let (wrk, mut cmd) = setup("frequency_select");
    cmd.extend(svec!["--limit", "0", "--select", "h2"]);
    let mut got: Vec<Vec<String>> = Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()) {
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    } else {
        panic!("run error!");
    }
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h2", "x", "1"],
        svec!["h2", "y", "2"],
        svec!["h2", "z", "3"],
    ];
    assert_eq!(got, expected);
}

// This tests that a frequency table computed by `xsv` is always the same
// as the frequency table computed in memory.

pub fn prop_frequency() {
    let rows = vec![
        svec!["x", "y", "z"],
        svec!["x", "", "z"],
        svec!["x", "m", "zl"],
    ];
    run_frequency("frequency", "prop_frequency", rows, false);
    // Run on really small values because we are incredibly careless
    // with allocation.
}

// This tests that running the frequency command on a CSV file with these two
// rows does not burst in flames:
//
//     \u{FEFF}
//     ""
//
// In this case, the `param_prop_frequency` just ignores this particular test.
// Namely, \u{FEFF} is the UTF-8 BOM, which is ignored by the underlying CSV
// reader.
pub fn frequency_bom() {
    let rows = vec![vec!["\u{FEFF}".to_string()], vec!["".to_string()]];
    assert!(run_frequency("frequency", "frequency_bom", rows, false))
}

// This tests that a frequency table computed by `xsv` (with an index) is
// always the same as the frequency table computed in memory.

pub fn prop_frequency_indexed() {
    let rows = vec![
        svec!["x", "y", "z"],
        svec!["x", "y", "z"],
        svec!["x", "m", "zl"],
    ];
    run_frequency("frequency", "prop_frequency_indxed", rows, true);

    // Run on really small values because we are incredibly careless
    // with allocation.
}

fn run_frequency(test_name: &str, fun_name: &str, rows: Vec<Vec<String>>, idx: bool) -> bool {
    let wrk = Workdir::new(test_name, fun_name);
    if idx {
        wrk.create_indexed(&format!("{}_in.csv", fun_name), rows.clone());
    } else {
        wrk.create(&format!("{}_in.csv", fun_name), rows.clone());
    }

    let mut cmd = wrk.command("frequency");
    let dir = wrk.result_dir();
    let path1 = &format!("{}/{}_in.csv", dir, fun_name);
    let outpath = &format!("{}/test-result-{}", dir, fun_name);
    cmd.extend(&[&path1, "-j", "4", "--limit", "0", "-o", &outpath]);
    if wrk.run(cmd) {
        let got_ftables = ftables_from_csv_path(outpath);
        let expected_ftables = ftables_from_rows(rows);
        assert_eq_ftables(&got_ftables, &expected_ftables)
    } else {
        panic!("run error!");
    }
}

type FTables = HashMap<String, Frequencies<String>>;

#[derive(Deserialize)]
struct FRow {
    field: String,
    value: String,
    count: usize,
}

fn ftables_from_rows(rows: Vec<Vec<String>>) -> FTables {
    let mut rows = rows;
    if rows.len() <= 1 {
        return HashMap::new();
    }

    let header = rows.remove(0);
    let mut ftables = HashMap::new();
    for field in header.iter() {
        ftables.insert(field.clone(), Frequencies::new());
    }
    for row in rows.into_iter() {
        for (i, mut field) in row.into_iter().enumerate() {
            field = field.trim().to_owned();
            if field.is_empty() {
                field = "(NULL)".to_owned();
            }
            ftables.get_mut(&header[i]).unwrap().add(field);
        }
    }
    ftables
}

fn ftables_from_csv_path(data: &str) -> FTables {
    let mut rdr = csv::Reader::from_path(data).unwrap();
    let mut ftables = HashMap::new();
    for frow in rdr.deserialize() {
        let frow: FRow = frow.unwrap();
        match ftables.entry(frow.field) {
            Entry::Vacant(v) => {
                let mut ftable = Frequencies::new();
                for _ in 0..frow.count {
                    ftable.add(frow.value.clone());
                }
                v.insert(ftable);
            }
            Entry::Occupied(mut v) => {
                for _ in 0..frow.count {
                    v.get_mut().add(frow.value.clone());
                }
            }
        }
    }
    ftables
}

fn freq_data<T>(ftable: &Frequencies<T>) -> Vec<(&T, u64)>
where
    T: ::std::hash::Hash + Ord + Clone,
{
    let mut freqs = ftable.most_frequent();
    freqs.sort();
    freqs
}

fn assert_eq_ftables(got: &FTables, expected: &FTables) -> bool {
    for (k, v) in got.iter() {
        assert_eq!(freq_data(v), freq_data(expected.get(k).unwrap()));
    }
    for (k, v) in expected.iter() {
        assert_eq!(freq_data(got.get(k).unwrap()), freq_data(v));
    }
    true
}
