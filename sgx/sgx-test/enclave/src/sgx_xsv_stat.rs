use std::prelude::v1::*;
use std::vec::Vec;
use workdir::Workdir;
use std::borrow::ToOwned;
use std::cmp;


macro_rules! stats_tests {
    ($name:ident, $field:expr, $rows:expr, $expect:expr) => (
        stats_tests!($name, $field, $rows, $expect, false);
    );
    ($name:ident, $field:expr, $rows:expr, $expect:expr, $nulls:expr) => (
       pub mod $name {
            use super::test_stats;

            stats_test_headers!($name, $field, $rows, $expect, $nulls);
            stats_test_no_headers!($name, $field, $rows, $expect, $nulls);
        }
    );
}

macro_rules! stats_test_headers {
    ($name:ident, $field:expr, $rows:expr, $expect:expr) => (
        stats_test_headers!($name, $field, $rows, $expect, false);
    );
    ($name:ident, $field:expr, $rows:expr, $expect:expr, $nulls:expr) => (

       pub fn headers_no_index() {
            let name = concat!(stringify!($name), "_headers_no_index");
            test_stats(name, "headers_no_index",$field, $rows, $expect, true, false, $nulls);
        }

       pub fn headers_index() {
            let name = concat!(stringify!($name), "_headers_index");
            test_stats(name,"headers_index", $field, $rows, $expect, true, true, $nulls);
        }
    );
}

macro_rules! stats_test_no_headers {
    ($name:ident, $field:expr, $rows:expr, $expect:expr) => (
        stats_test_no_headers!($name, $field, $rows, $expect, false);
    );
    ($name:ident, $field:expr, $rows:expr, $expect:expr, $nulls:expr) => (

     pub fn no_headers_no_index() {
            let name = concat!(stringify!($name), "_no_headers_no_index");
            test_stats(name, "no_headers_no_index",$field, $rows, $expect, false, false, $nulls);
        }

    
       pub fn no_headers_index() {
            let name = concat!(stringify!($name), "_no_headers_index");
            test_stats(name,"no_headers_index",$field, $rows, $expect, false, true, $nulls);
        }
    );
}

fn test_stats(name: &str,fun_name:&str, field: &str, rows: &[&str], expected: &str,
                 headers: bool, use_index: bool, nulls: bool)
         {
    let (wrk, mut cmd) = setup(name,fun_name,rows, headers, use_index, nulls);
    let field_val = get_field_value(&wrk, &mut cmd, field);

    let len = cmp::min(10, cmp::min(field_val.len(), expected.len()));
    assert_eq!(&field_val[0..len], &expected[0..len]);
}

fn setup<'a>(name: &'a str,fun_name:&'a str, rows: &[&str], headers: bool,
            use_index: bool, nulls: bool) -> (Workdir, Vec<String>)
     {
    let wrk = Workdir::new("stats",&format!("{}_{}",name,fun_name));
    let mut data: Vec<Vec<String>> =
        rows.iter().map(|&s| vec![s.to_owned()]).collect();
    if headers { data.insert(0, svec!["header"]); }
    if use_index {
        wrk.create_indexed(&format!("{}_{}_in.csv",name,fun_name), data);
    } else {
        wrk.create(&format!("{}_{}_in.csv",name,fun_name), data);
    }
    let dir = wrk.result_dir();
    let input = format!("{}/{}_{}_in.csv",dir,name,fun_name);
    let mut cmd:Vec<String> = wrk.command("stats").into_iter().map(|s| s.to_string()).collect();
    cmd.push(input);
    cmd.push("-o".to_string());
    cmd.push(format!("{}/test-result-{}_{}",dir,name,fun_name));
    if !headers { cmd.push("--no-headers".to_string()); }
    if nulls { cmd.push("--nulls".to_string()); }

    (wrk, cmd)
}

fn get_field_value(wrk: &Workdir, cmd: &mut Vec<String>, field: &str)
                  -> String {
    if field == "median" { cmd.push("--median".to_string()); }
    if field == "cardinality" { cmd.push("--cardinality".to_string()); }
    if field == "mode" { cmd.push("--mode".to_string()); }
    let mut rows: Vec<Vec<String>> =Vec::new();

    if wrk.run(cmd.into_iter().map(|s| s.as_str()).collect()){
        rows.extend(wrk.read_from_file(false).unwrap_or(vec![vec![]]))
    }else{
        panic!("run error!");
    }
   // let mut rows: Vec<Vec<String>> = wrk.read_stdout(cmd);
    let headers = rows.remove(0);
    for row in rows.iter() {
        for (h, val) in headers.iter().zip(row.iter()) {
            if &**h == field {
                return val.clone();
            }
        }
    }
    panic!("BUG: Could not find field '{}' in headers '{:?}' \
            for command '{:?}'.", field, headers, cmd);
}
macro_rules! run{
    ($name:ident) => (
       $name::no_headers_no_index();
       $name::no_headers_index(); 
       $name::headers_no_index();
       $name::headers_index();

    );

}
pub fn tests(){
run!(stats_infer_unicode);
run!(stats_infer_int);
run!(stats_infer_float);
run!(stats_infer_null);
run!(stats_infer_unicode_null);
run!(stats_infer_int_null);
run!(stats_infer_float_null);
run!(stats_infer_null_unicode);
run!(stats_infer_null_int);
run!(stats_infer_null_float);
run!(stats_infer_int_unicode);
run!(stats_infer_unicode_int);
run!(stats_infer_int_float);
run!(stats_infer_float_int);
run!(stats_infer_null_int_float_unicode);
run!(stats_no_mean);
run!(stats_no_stddev);
run!(stats_no_median);
run!(stats_no_mode);
run!(stats_null_mean);
run!(stats_null_stddev);
run!(stats_null_median);
run!(stats_null_mode);
run!(stats_includenulls_null_mean);
run!(stats_includenulls_null_stddev);
run!(stats_includenulls_null_median);
run!(stats_includenulls_null_mode);
run!(stats_includenulls_mean);
run!(stats_sum_integers);
run!(stats_sum_floats);
run!(stats_sum_mixed1);
run!(stats_sum_mixed2);
run!(stats_sum_mixed3);
run!(stats_sum_nulls1);
run!(stats_sum_nulls2);
run!(stats_min);
run!(stats_max);
run!(stats_min_mix);
run!(stats_max_mix);
run!(stats_min_null);
run!(stats_max_null);
run!(stats_len_min);
run!(stats_len_max);
run!(stats_len_min_null);
run!(stats_len_max_null);
run!(stats_mean);
run!(stats_stddev);
run!(stats_mean_null);
run!(stats_stddev_null);    
run!(stats_mean_mix);
run!(stats_stddev_mix);    
run!(stats_cardinality);
run!(stats_mode);
run!(stats_mode_null);
run!(stats_median);
run!(stats_median_null);
run!(stats_median_even);
run!(stats_median_even_null);
run!(stats_median_mix);
}
stats_tests!(stats_infer_unicode, "type", &["a"], "Unicode");
stats_tests!(stats_infer_int, "type", &["1"], "Integer");
stats_tests!(stats_infer_float, "type", &["1.2"], "Float");
stats_tests!(stats_infer_null, "type", &[""], "NULL");
stats_tests!(stats_infer_unicode_null, "type", &["a", ""], "Unicode");
stats_tests!(stats_infer_int_null, "type", &["1", ""], "Integer");
stats_tests!(stats_infer_float_null, "type", &["1.2", ""], "Float");
stats_tests!(stats_infer_null_unicode, "type", &["", "a"], "Unicode");
stats_tests!(stats_infer_null_int, "type", &["", "1"], "Integer");
stats_tests!(stats_infer_null_float, "type", &["", "1.2"], "Float");
stats_tests!(stats_infer_int_unicode, "type", &["1", "a"], "Unicode");
stats_tests!(stats_infer_unicode_int, "type", &["a", "1"], "Unicode");
stats_tests!(stats_infer_int_float, "type", &["1", "1.2"], "Float");
stats_tests!(stats_infer_float_int, "type", &["1.2", "1"], "Float");
stats_tests!(stats_infer_null_int_float_unicode, "type",
             &["", "1", "1.2", "a"], "Unicode");

stats_tests!(stats_no_mean, "mean", &["a"], "");
stats_tests!(stats_no_stddev, "stddev", &["a"], "");
stats_tests!(stats_no_median, "median", &["a"], "");
stats_tests!(stats_no_mode, "mode", &["a", "b"], "N/A");

stats_tests!(stats_null_mean, "mean", &[""], "");
stats_tests!(stats_null_stddev, "stddev", &[""], "");
stats_tests!(stats_null_median, "median", &[""], "");
stats_tests!(stats_null_mode, "mode", &[""], "N/A");

stats_tests!(stats_includenulls_null_mean, "mean", &[""], "", true);
stats_tests!(stats_includenulls_null_stddev, "stddev", &[""], "", true);
stats_tests!(stats_includenulls_null_median, "median", &[""], "", true);
stats_tests!(stats_includenulls_null_mode, "mode", &[""], "N/A", true);

stats_tests!(stats_includenulls_mean,
             "mean", &["5", "", "15", "10"], "7.5", true);

stats_tests!(stats_sum_integers, "sum", &["1", "2"], "3");
stats_tests!(stats_sum_floats, "sum", &["1.5", "2.8"], "4.3");
stats_tests!(stats_sum_mixed1, "sum", &["1.5", "2"], "3.5");
stats_tests!(stats_sum_mixed2, "sum", &["2", "1.5"], "3.5");
stats_tests!(stats_sum_mixed3, "sum", &["1.5", "hi", "2.8"], "4.3");
stats_tests!(stats_sum_nulls1, "sum", &["1", "", "2"], "3");
stats_tests!(stats_sum_nulls2, "sum", &["", "1", "2"], "3");

stats_tests!(stats_min, "min", &["2", "1.1"], "1.1");
stats_tests!(stats_max, "max", &["2", "1.1"], "2");
stats_tests!(stats_min_mix, "min", &["2", "a", "1.1"], "1.1");
stats_tests!(stats_max_mix, "max", &["2", "a", "1.1"], "a");
stats_tests!(stats_min_null, "min", &["", "2", "1.1"], "1.1");
stats_tests!(stats_max_null, "max", &["2", "1.1", ""], "2");

stats_tests!(stats_len_min, "min_length", &["aa", "a"], "1");
stats_tests!(stats_len_max, "max_length", &["a", "aa"], "2");
stats_tests!(stats_len_min_null, "min_length", &["", "aa", "a"], "0");
stats_tests!(stats_len_max_null, "max_length", &["a", "aa", ""], "2");

stats_tests!(stats_mean, "mean", &["5", "15", "10"], "10");
stats_tests!(stats_stddev, "stddev", &["1", "2", "3"], "0.816496580927726");
stats_tests!(stats_mean_null, "mean", &["", "5", "15", "10"], "10");
stats_tests!(stats_stddev_null, "stddev", &["1", "2", "3", ""],
             "0.816496580927726");
stats_tests!(stats_mean_mix, "mean", &["5", "15.1", "9.9"], "10");
stats_tests!(stats_stddev_mix, "stddev", &["1", "2.1", "2.9"],
             "0.7788880963698614");

stats_tests!(stats_cardinality, "cardinality", &["a", "b", "a"], "2");
stats_tests!(stats_mode, "mode", &["a", "b", "a"], "a");
stats_tests!(stats_mode_null, "mode", &["", "a", "b", "a"], "a");
stats_tests!(stats_median, "median", &["1", "2", "3"], "2");
stats_tests!(stats_median_null, "median", &["", "1", "2", "3"], "2");
stats_tests!(stats_median_even, "median", &["1", "2", "3", "4"], "2.5");
stats_tests!(stats_median_even_null, "median",
             &["", "1", "2", "3", "4"], "2.5");
stats_tests!(stats_median_mix, "median", &["1", "2.5", "3"], "2.5");

mod stats_infer_nothing {
    // Only test CSV data with headers.
    // Empty CSV data with no headers won't produce any statistical analysis.
    use super::test_stats;
    stats_test_headers!(stats_infer_nothing, "type", &[], "NULL");
}

mod stats_zero_cardinality {
    use super::test_stats;
    stats_test_headers!(stats_zero_cardinality, "cardinality", &[], "0");
}

mod stats_zero_mode {
    use super::test_stats;
    stats_test_headers!(stats_zero_mode, "mode", &[], "N/A");
}

mod stats_zero_mean {
    use super::test_stats;
    stats_test_headers!(stats_zero_mean, "mean", &[], "");
}

mod stats_zero_median {
    use super::test_stats;
    stats_test_headers!(stats_zero_median, "median", &[], "");
}

mod stats_header_fields {
    use super::test_stats;
    stats_test_headers!(stats_header_field_name, "field", &["a"], "header");
    stats_test_no_headers!(stats_header_no_field_name, "field", &["a"], "0");
}

