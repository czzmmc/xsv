// Copyright (C) 2017-2018 Baidu, Inc. All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//  * Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in
//    the documentation and/or other materials provided with the
//    distribution.
//  * Neither the name of Baidu, Inc., nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#![allow(dead_code)]
#![crate_name = "helloworldsampleenclave"]
#![crate_type = "staticlib"]
#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

extern crate sgx_types;

#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;

extern crate sgx_tunittest;

use sgx_tunittest::*;
use sgx_types::*;
use std::io::{self, Write};
use std::prelude::v1::*;
use std::slice;
use std::string::String;
use std::vec::Vec;
extern crate csv;
extern crate serde;
extern crate serde_derive;
extern crate stats;
extern crate uuid;
extern crate xsv;
macro_rules! rassert_eq {
    ($given:expr, $expected:expr) => {{
        assert_eq!($given, $expected);
        true
    }};
}

macro_rules! svec[
    ($($x:expr),*) => (
        vec![$($x),*].into_iter()
                     .map(|s: &'static str| s.to_string())
                     .collect::<Vec<String>>()
    );
    ($($x:expr,)*) => (svec![$($x),*]);
];

mod sgx_test_count;
mod sgx_xsv_cat;
mod sgx_xsv_frequency;
mod sgx_xsv_join;
mod sgx_xsv_select;
mod sgx_xsv_slice;
mod sgx_xsv_sort;
mod sgx_xsv_stat;
mod workdir;
#[no_mangle]
pub extern "C" fn say_something(some_string: *const u8, some_len: usize) -> sgx_status_t {
    let str_slice = unsafe { slice::from_raw_parts(some_string, some_len) };
    let _ = io::stdout().write(str_slice);

    // A sample &'static string
    let rust_raw_string = "This is a in-Enclave ";
    // An array
    let word: [u8; 4] = [82, 117, 115, 116];
    // An vector
    let word_vec: Vec<u8> = vec![32, 115, 116, 114, 105, 110, 103, 33];

    // Construct a string from &'static string
    let mut hello_string = String::from(rust_raw_string);

    // Iterate on word array
    for c in word.iter() {
        hello_string.push(*c as char);
    }

    // Rust style convertion
    hello_string += String::from_utf8(word_vec).expect("Invalid UTF-8").as_str();

    // Ocall to normal world for output
    println!("{}", &hello_string);
    rsgx_unit_tests!(
        workdir::test_terasort,
        workdir::test_multijoin,
        workdir::simple_test_split,
        workdir::simple_test_headers,
        workdir::simple_test_partition,
        sgx_xsv_cat::cat_rows_headers,
        sgx_xsv_cat::cat_cols_headers,
        sgx_xsv_cat::cat_cols_no_pad,
        sgx_xsv_cat::cat_cols_pad,
        sgx_test_count::prop_count,
        sgx_test_count::prop_count_headers,
        sgx_test_count::prop_count_indexed,
        sgx_test_count::prop_count_indexed_headers,
        sgx_xsv_stat::tests,
        sgx_xsv_frequency::prop_frequency,
        sgx_xsv_frequency::frequency_bom,
        sgx_xsv_frequency::prop_frequency_indexed,
        sgx_xsv_frequency::frequency_no_headers,
        sgx_xsv_frequency::frequency_no_nulls,
        sgx_xsv_frequency::frequency_nulls,
        sgx_xsv_frequency::frequency_limit,
        sgx_xsv_frequency::frequency_asc,
        sgx_xsv_frequency::frequency_select,
        sgx_xsv_join::join_inner::headers,
        sgx_xsv_join::join_inner::no_headers,
        sgx_xsv_join::join_outer_left::headers,
        sgx_xsv_join::join_outer_left::no_headers,
        sgx_xsv_join::join_outer_right::headers,
        sgx_xsv_join::join_outer_right::no_headers,
        sgx_xsv_join::join_outer_full::headers,
        sgx_xsv_join::join_outer_full::no_headers,
        sgx_xsv_join::join_inner_issue11,
        sgx_xsv_join::join_cross,
        sgx_xsv_join::join_cross_no_headers,
        sgx_xsv_select::select_simple::headers,
        sgx_xsv_select::select_simple::no_headers,
        sgx_xsv_select::select_simple_idx::headers,
        sgx_xsv_select::select_simple_idx::no_headers,
        sgx_xsv_select::select_simple_idx_2::headers,
        sgx_xsv_select::select_simple_idx_2::no_headers,
        sgx_xsv_select::select_quoted::headers,
        sgx_xsv_select::select_quoted::no_headers,
        sgx_xsv_select::select_quoted_idx::headers,
        sgx_xsv_select::select_quoted_idx::no_headers,
        sgx_xsv_select::select_range::headers,
        sgx_xsv_select::select_range::no_headers,
        sgx_xsv_select::select_range_multi::headers,
        sgx_xsv_select::select_range_multi::no_headers,
        sgx_xsv_select::select_range_multi_idx::headers,
        sgx_xsv_select::select_range_multi_idx::no_headers,
        sgx_xsv_select::select_reverse::headers,
        sgx_xsv_select::select_reverse::no_headers,
        sgx_xsv_select::select_not::headers,
        sgx_xsv_select::select_not::no_headers,
        sgx_xsv_select::select_not_range::headers,
        sgx_xsv_select::select_not_range::no_headers,
        sgx_xsv_select::select_duplicate::headers,
        sgx_xsv_select::select_duplicate::no_headers,
        sgx_xsv_select::select_duplicate_range::headers,
        sgx_xsv_select::select_duplicate_range::no_headers,
        sgx_xsv_select::select_duplicate_range_reverse::headers,
        sgx_xsv_select::select_duplicate_range_reverse::no_headers,
        sgx_xsv_select::select_range_no_end::headers,
        sgx_xsv_select::select_range_no_end::no_headers,
        sgx_xsv_select::select_range_no_start::headers,
        sgx_xsv_select::select_range_no_start::no_headers,
        sgx_xsv_select::select_range_no_end_cat::headers,
        sgx_xsv_select::select_range_no_end_cat::no_headers,
        sgx_xsv_select::select_range_no_start_cat::headers,
        sgx_xsv_select::select_range_no_start_cat::no_headers,
        sgx_xsv_sort::prop_sort_headers,
        sgx_xsv_sort::prop_sort_no_headers,
        sgx_xsv_sort::sort_select,
        sgx_xsv_sort::sort_numeric,
        sgx_xsv_sort::sort_numeric_non_natural,
        sgx_xsv_sort::sort_reverse,
        sgx_xsv_slice::slice_simple::headers_no_index,
        sgx_xsv_slice::slice_simple::no_headers_no_index,
        sgx_xsv_slice::slice_simple::headers_index,
        sgx_xsv_slice::slice_simple::no_headers_index,
        sgx_xsv_slice::slice_simple::headers_no_index_len,
        sgx_xsv_slice::slice_simple::no_headers_no_index_len,
        sgx_xsv_slice::slice_simple::headers_index_len,
        sgx_xsv_slice::slice_simple::no_headers_index_len,
        sgx_xsv_slice::slice_simple_2::headers_no_index,
        sgx_xsv_slice::slice_simple_2::no_headers_no_index,
        sgx_xsv_slice::slice_simple_2::headers_index,
        sgx_xsv_slice::slice_simple_2::no_headers_index,
        sgx_xsv_slice::slice_simple_2::headers_no_index_len,
        sgx_xsv_slice::slice_simple_2::no_headers_no_index_len,
        sgx_xsv_slice::slice_simple_2::headers_index_len,
        sgx_xsv_slice::slice_simple_2::no_headers_index_len,
        sgx_xsv_slice::slice_no_start::headers_no_index,
        sgx_xsv_slice::slice_no_start::no_headers_no_index,
        sgx_xsv_slice::slice_no_start::headers_index,
        sgx_xsv_slice::slice_no_start::no_headers_index,
        sgx_xsv_slice::slice_no_start::headers_no_index_len,
        sgx_xsv_slice::slice_no_start::no_headers_no_index_len,
        sgx_xsv_slice::slice_no_start::headers_index_len,
        sgx_xsv_slice::slice_no_start::no_headers_index_len,
        sgx_xsv_slice::slice_no_end::headers_no_index,
        sgx_xsv_slice::slice_no_end::no_headers_no_index,
        sgx_xsv_slice::slice_no_end::headers_index,
        sgx_xsv_slice::slice_no_end::no_headers_index,
        sgx_xsv_slice::slice_no_end::headers_no_index_len,
        sgx_xsv_slice::slice_no_end::no_headers_no_index_len,
        sgx_xsv_slice::slice_no_end::headers_index_len,
        sgx_xsv_slice::slice_no_end::no_headers_index_len,
        sgx_xsv_slice::slice_all::headers_no_index,
        sgx_xsv_slice::slice_all::no_headers_no_index,
        sgx_xsv_slice::slice_all::headers_index,
        sgx_xsv_slice::slice_all::no_headers_index,
        sgx_xsv_slice::slice_all::headers_no_index_len,
        sgx_xsv_slice::slice_all::no_headers_no_index_len,
        sgx_xsv_slice::slice_all::headers_index_len,
        sgx_xsv_slice::slice_all::no_headers_index_len,
        sgx_xsv_slice::slice_index,
        sgx_xsv_slice::slice_index_no_headers,
        sgx_xsv_slice::slice_index_withindex,
        sgx_xsv_slice::slice_index_no_headers_withindex,
    );

    sgx_status_t::SGX_SUCCESS
}
