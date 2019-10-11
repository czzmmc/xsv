use std::prelude::v1::*;
use std::vec;
use workdir::Workdir;

fn no_headers(mut cmd:Vec<&str>){
    cmd.push("--no-headers");
}

fn pad(mut cmd: Vec<&str>){
    cmd.push("--pad");
}
fn mofigy_cmd<'a>(mut cmd:Vec<&'a str>,arg:&Vec<&'a str>)->Vec<&'a str>{
    cmd.extend(arg);
    cmd
}
fn run_cat(test_name: &str,fun_name:&str, which: &str, rows1: Vec<Vec<String>>, rows2: Vec<Vec<String>>,header:bool,pad_flag:bool ) -> Vec<Vec<String>>
 {
    let wrk = Workdir::new(test_name,fun_name);
    
    wrk.create(&format!("{}_in1.csv",fun_name), rows1);
    wrk.create(&format!("{}_in2.csv",fun_name), rows2);

    let cmd = wrk.command("cat");
    let dir = wrk.result_dir();
    let path1 = &format!("{}/{}_in1.csv",dir,fun_name);
    let path2= &format!("{}/{}_in2.csv",dir,fun_name);
   
    let outpath =&format!("{}/test-result-{}",dir,fun_name);
    let mut cmd =mofigy_cmd(cmd,&vec![which,&path1,&path2,"-o",&outpath]);
    if !header{
        cmd.push("--no-headers")
    }
    if pad_flag{
       cmd.push("--pad");
    }
    if wrk.run(cmd){
        wrk.read_from_file(false).unwrap_or(vec![vec![]])
    }else{
        panic!("run error!");
    }
}

pub fn cat_rows_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h1", "h2"], svec!["y", "z"]];

    let mut expected = rows1.clone();
    expected.extend(rows2.clone().into_iter().skip(1));

    let got: Vec<Vec<String>> = run_cat("cat", "cat_rows_headers","rows",
                                        rows1, rows2,true,false);
    assert_eq!(got, expected);
}

pub fn cat_rows_space() {
    let rows = vec![svec!["\u{0085}"]];
    let expected = rows.clone();
    let (rows1, rows2) =
        if rows.is_empty() {
            (vec![], vec![])
        } else {
            let (rows1, rows2) = rows.split_at(rows.len() / 2);
            (rows1.to_vec(), rows2.to_vec())
        };
    let got: Vec<Vec<String>> =
        run_cat("cat", "cat_rows_space", "rows", rows1, rows2,true,false);
    assert_eq!(got, expected);
}



pub fn cat_cols_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h3", "h4"], svec!["y", "z"]];

    let expected = vec![
        svec!["h1", "h2", "h3", "h4"],
        svec!["a", "b", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat","cat_cols_headers", "columns",
                                        rows1, rows2, true,false);
    assert_eq!(got, expected);
}


pub fn cat_cols_no_pad() {
    let rows1 = vec![svec!["a", "b"]];
    let rows2 = vec![svec!["y", "z"], svec!["y", "z"]];

    let expected = vec![
        svec!["a", "b", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat","cat_cols_no_pad", "columns",
                                        rows1, rows2, true,false);
    assert_eq!(got, expected);
}


pub fn cat_cols_pad() {
    let rows1 = vec![svec!["a", "b"]];
    let rows2 = vec![svec!["y", "z"], svec!["y", "z"]];

    let expected = vec![
        svec!["a", "b", "y", "z"],
        svec!["", "", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat","cat_cols_pad", "columns",
                                        rows1, rows2, true,true);
    assert_eq!(got, expected);
}
