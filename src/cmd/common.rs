use crate::cmd::sort::{iter_cmp, iter_cmp_num};
use csv;
use select::Selection;
use std::cmp;
use std::prelude::v1::*;
use std::str::from_utf8;
use {CliError, CliResult};
type ByteString = Vec<u8>;

pub fn check_num_type(sample: &[ByteString]) -> CliResult<()> {
    let keys = sample
        .iter()
        .map(|x| x.as_slice())
        .collect::<Vec<_>>()
        .into_iter();
    for key in keys {
        let string = match from_utf8(key) {
            Err(_) => return Err(CliError::Other("parse error.".to_string())),
            Ok(s) => s,
        };
        if string.parse::<i64>().is_ok() {
            continue;
        }
        if string.parse::<f64>().is_ok() {
            continue;
        }
        return Err(CliError::Other(
            "parse error.Filed's type is unicode".to_string(),
        ));
    }

    Ok(())
}

pub fn get_row_key(sel: &Selection, row: &csv::ByteRecord, casei: bool) -> Vec<ByteString> {
    sel.select(row).map(|v| transform(&v, casei)).collect()
}
pub fn cmp_key(key1: &[ByteString], key2: &[ByteString], flag_numeric: bool) -> cmp::Ordering {
    if flag_numeric {
        let k1 = key1
            .iter()
            .map(|x| x.as_slice())
            .collect::<Vec<_>>()
            .into_iter();
        let k2 = key2
            .iter()
            .map(|x| x.as_slice())
            .collect::<Vec<_>>()
            .into_iter();
        match iter_cmp_num(k1, k2) {
            cmp::Ordering::Equal => cmp::Ordering::Equal,
            cmp::Ordering::Less => cmp::Ordering::Less,
            cmp::Ordering::Greater => cmp::Ordering::Greater,
        }
    } else {
        match iter_cmp(key1.iter(), key2.iter()) {
            cmp::Ordering::Equal => cmp::Ordering::Equal,
            cmp::Ordering::Less => cmp::Ordering::Less,
            cmp::Ordering::Greater => cmp::Ordering::Greater,
        }
    }
}
pub fn transform(bs: &[u8], casei: bool) -> ByteString {
    match from_utf8(bs) {
        Err(_) => bs.to_vec(),
        Ok(s) => {
            if !casei {
                s.trim().as_bytes().to_vec()
            } else {
                let norm: String = s
                    .trim()
                    .chars()
                    .map(|c| c.to_lowercase().next().unwrap())
                    .collect();
                norm.into_bytes()
            }
        }
    }
}

pub fn transform_string(bs: &[u8], casei: bool) -> CliResult<String> {
    match from_utf8(bs) {
        Err(_) => Err(CliError::Other(
            "parse error.Filed's type is not utf8".to_string(),
        )),
        Ok(s) => {
            if !casei {
                Ok(s.trim().to_string())
            } else {
                Ok(s.trim()
                    .chars()
                    .map(|c| c.to_lowercase().next().unwrap())
                    .collect())
            }
        }
    }
}
