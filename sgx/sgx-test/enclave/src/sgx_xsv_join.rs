use workdir::Workdir;
use std::prelude::v1::*;
use std::vec::Vec;
use std::string::String;
// This macro takes *two* identifiers: one for the test with headers
// and another for the test without headers.
macro_rules! join_test {
    ($name:ident, $fun:expr) => (
       pub mod $name {
            use std::prelude::v1::*;
            use std::vec::Vec;
            use std::string::String;
            use workdir::Workdir;
            use super::{make_rows, setup};

            pub fn headers() {
                let wrk = setup(stringify!($name), true);
                let dir = wrk.result_dir();
                let input1 = format!("{}/{}_cities.csv",dir,stringify!($name));
                let input2 = format!("{}/{}_places.csv",dir,stringify!($name));
                let outpath = format!("{}/test-result-{}",dir,stringify!($name));
                let mut cmd:Vec<String> = wrk.command("join").into_iter().map(|s| s.to_string()).collect();
                cmd.extend(vec!["city".to_string(), input1, "city".to_string(), input2]);
                cmd.extend(vec!["-o".to_string(),outpath]);
                $fun(wrk, cmd, true);
            }

        
            pub fn no_headers() {
                // let n = stringify!(concat_idents!($name, _no_headers));
                let n ="test";
                let wrk = setup(n, false);
                let dir = wrk.result_dir();
                let input1 = format!("{}/{}_cities.csv",dir,n);
                let input2 = format!("{}/{}_places.csv",dir,n);
                let outpath = format!("{}/test-result-{}",dir,n);
                let mut cmd:Vec<String> = wrk.command("join").into_iter().map(|s| s.to_string()).collect();
                cmd.push("--no-headers".to_string());
                cmd.extend(vec!["1".to_string(), input1, "1".to_string(), input2]);
                cmd.extend(vec!["-o".to_string(),outpath]);
                $fun(wrk, cmd, false);
            }
        }
    );
}

fn setup(name: &str, headers: bool) -> Workdir {
    let mut cities = vec![
        svec!["Boston", "MA"],
        svec!["New York", "NY"],
        svec!["San Francisco", "CA"],
        svec!["Buffalo", "NY"],
    ];
    let mut places = vec![
        svec!["Boston", "Logan Airport"],
        svec!["Boston", "Boston Garden"],
        svec!["Buffalo", "Ralph Wilson Stadium"],
        svec!["Orlando", "Disney World"],
    ];
    if headers { cities.insert(0, svec!["city", "state"]); }
    if headers { places.insert(0, svec!["city", "place"]); }

    let wrk = Workdir::new("join",name);
    wrk.create(&format!("{}_cities.csv",name), cities);
    wrk.create(&format!("{}_places.csv",name), places);
    wrk
}

fn make_rows(headers: bool, rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut all_rows = vec![];
    if headers {
        all_rows.push(svec!["city", "state", "city", "place"]);
    }
    all_rows.extend(rows.into_iter());
    all_rows
}

join_test!(join_inner,|wrk: Workdir, cmd:Vec<String>, headers: bool| {
    let mut got: Vec<Vec<String>>=Vec::new();
        if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_left,
           |wrk: Workdir, mut cmd: Vec<String>, headers: bool| {
    cmd.push("--left".to_string());
    let mut got: Vec<Vec<String>>=Vec::new();
        if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_right,
           |wrk: Workdir, mut cmd: Vec<String>, headers: bool| {
        cmd.push("--right".to_string());
        let mut got: Vec<Vec<String>>=Vec::new();
        if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_full,
           |wrk: Workdir, mut cmd:Vec<String>, headers: bool| {
        cmd.push("--full".to_string());
        let mut got: Vec<Vec<String>>=Vec::new();
        if wrk.run((&cmd).iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
});


pub fn join_inner_issue11() {
    let a = vec![
        svec!["1", "2"],
        svec!["3", "4"],
        svec!["5", "6"],
    ];
    let b = vec![
        svec!["2", "1"],
        svec!["4", "3"],
        svec!["6", "5"],
    ];

    let wrk = Workdir::new("join","join_inner_issue11");
    wrk.create("join_inner_issue11_a.csv", a);
    wrk.create("join_inner_issue11_b.csv", b);
    let dir = wrk.result_dir();
    let input1 = format!("{}/join_inner_issue11_a.csv",dir);
    let input2 = format!("{}/join_inner_issue11_b.csv",dir);
    let outpath = format!("{}/test-result-join_inner_issue11",dir);
    
    let mut cmd:Vec<String> = wrk.command("join").into_iter().map(|s| s.to_string()).collect();
    cmd.extend(vec!["1,2".to_string(), input1, "2,1".to_string(), input2]);
    cmd.extend(vec!["-o".to_string(),outpath]);
    let mut got: Vec<Vec<String>> =Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = vec![
        svec!["1", "2", "2", "1"],
        svec!["3", "4", "4", "3"],
        svec!["5", "6", "6", "5"],
    ];
    assert_eq!(got, expected);
}

pub fn join_cross() {
    let wrk = Workdir::new("join","join_cross");
    wrk.create("join_cross_letters.csv",
               vec![svec!["h1", "h2"], svec!["a", "b"], svec!["c", "d"]]);
    wrk.create("join_cross_numbers.csv",
               vec![svec!["h3", "h4"], svec!["1", "2"], svec!["3", "4"]]);
    let dir = wrk.result_dir();
    let input1 = format!("{}/join_cross_letters.csv",dir);
    let input2= format!("{}/join_cross_numbers.csv",dir);
    let outpath = format!("{}/test-result-join_cross",dir);
    let mut cmd:Vec<String> = wrk.command("join").into_iter().map(|s| s.to_string()).collect();
    cmd.push("--cross".to_string());
    cmd.extend(vec!["".to_string(), input1, "".to_string(), input2]);
    cmd.extend(vec!["-o".to_string(),outpath]);
    let mut got: Vec<Vec<String>> =Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
    let expected = vec![
        svec!["h1", "h2", "h3", "h4"],
        svec!["a", "b", "1", "2"],
        svec!["a", "b", "3", "4"],
        svec!["c", "d", "1", "2"],
        svec!["c", "d", "3", "4"],
    ];
    assert_eq!(got, expected);
}

pub fn join_cross_no_headers() {
    let wrk = Workdir::new("join","join_cross_no_headers");
    wrk.create("join_cross_no_headers_letters.csv", vec![svec!["a", "b"], svec!["c", "d"]]);
    wrk.create("join_cross_no_headers_numbers.csv", vec![svec!["1", "2"], svec!["3", "4"]]);
    let dir = wrk.result_dir();
    let input1 = format!("{}/join_cross_no_headers_letters.csv",dir);
    let input2 = format!("{}/join_cross_no_headers_numbers.csv",dir);
    let outpath = format!("{}/test-result-join_cross_no_headers",dir);
    let mut cmd:Vec<String> = wrk.command("join").into_iter().map(|s| s.to_string()).collect();
    cmd.extend(vec!["--cross".to_string(),"--no-headers".to_string(),"".to_string(), input1, "".to_string(), input2]);
    cmd.extend(vec!["-o".to_string(),outpath]);
    let mut got: Vec<Vec<String>> =Vec::new();
    if wrk.run((&cmd).into_iter().map(|s| s.as_str()).collect()){
        got.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }

    let expected = vec![
        svec!["a", "b", "1", "2"],
        svec!["a", "b", "3", "4"],
        svec!["c", "d", "1", "2"],
        svec!["c", "d", "3", "4"],
    ];
    assert_eq!(got, expected);
}
