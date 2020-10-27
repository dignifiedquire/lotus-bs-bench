use std::ffi::{CStr, CString};
use std::mem;
use std::ptr;
use std::slice;

use libc::*;

use faster_rs as faster;

pub type FasterDb =  faster::FasterKv;

fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    unsafe {
        *vallen = v.len();
    }
    let mut bsv = v.into_boxed_slice();
    let val = bsv.as_mut_ptr() as *mut _;
    mem::forget(bsv);
    val
}

#[no_mangle]
pub unsafe extern "C" fn f_open_db(
    table_size: u64,
    log_size: u64,
    storage: *const c_char,
    log_mutable_fraction: f64,
    pre_allocate_log: bool,
) -> *mut FasterDb {
    if let Ok(db) = faster::FasterKvBuilder::new(table_size, log_size)
        .with_disk(dbg!(CStr::from_ptr(storage).to_str().unwrap()))
        .with_log_mutable_fraction(log_mutable_fraction)
        .set_pre_allocate_log(pre_allocate_log).build() {
            return Box::into_raw(Box::new(db));
        }
    ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn f_close(faster: *mut FasterDb) {
    drop(Box::from_raw(faster))
}
    
/// Free a buffer originally allocated by rust
#[no_mangle]
pub unsafe extern "C" fn f_free_buf(buf: *mut c_char, sz: size_t) {
    drop(Vec::from_raw_parts(buf, sz, sz));
}

/// Set a key to a value.
#[no_mangle]
pub unsafe extern "C" fn f_set(
    db: *const FasterDb,
    key: *const c_uchar,
    keylen: size_t,
    val: *const c_uchar,
    vallen: size_t,
    serial_number: size_t,
) -> u8 {
    // TODO: fix faster-rs to avoid allocations
    let k = slice::from_raw_parts(key, keylen).to_vec();
    let v = slice::from_raw_parts(val, vallen).to_vec();

    (*db).upsert(&k, &v, serial_number as u64)
}

#[no_mangle]
pub unsafe extern "C" fn f_has(
    db: *const FasterDb,
    key: *const c_char,
    keylen: size_t,
    serial_number: size_t,
) -> size_t {
    let k: Vec<u8> = slice::from_raw_parts(key as *const u8, keylen).to_vec();
    let (res, _recv): (_, std::sync::mpsc::Receiver<Vec<u8>>) = (*db).read(&k, serial_number as u64);
    match res {
        faster::status::OK => {
            1
        }
        faster::status::NOT_FOUND => {
            0
        }
        _ => 0
    }
}

/// Get the value of a key.
#[no_mangle]
pub unsafe extern "C" fn f_get(
    db: *const FasterDb,
    key: *const c_char,
    keylen: size_t,
    val: *mut *const c_char,
    vallen: *mut size_t,
    serial_number: size_t
) -> u8 {
    let k: Vec<u8> = slice::from_raw_parts(key as *const u8, keylen).to_vec();
    let (res, recv): (_, std::sync::mpsc::Receiver<Vec<u8>>) = (*db).read(&k, serial_number as u64);
    match res {
        faster::status::OK => {
            let v = recv.recv().unwrap();
            *val = leak_buf(v, vallen);
        }
        _ => {}
    }

    res
}

#[no_mangle]
pub unsafe extern "C" fn f_get_len(
    db: *const FasterDb,
    key: *const c_char,
    keylen: size_t,
    serial_number: size_t,
) -> c_long {
    let k: Vec<u8> = slice::from_raw_parts(key as *const u8, keylen).to_vec();
    let (res, recv): (_, std::sync::mpsc::Receiver<Vec<u8>>) = (*db).read(&k, serial_number as u64);
    match res {
        faster::status::OK => {
            let v = recv.recv().unwrap();
            v.len() as c_long
        }
        _ => -1
    }
}

/// Delete the value of a key.
#[no_mangle]
pub unsafe extern "C" fn f_del(
    db: *const FasterDb,
    key: *const c_char,
    keylen: size_t,
    serial_number: size_t,
) -> u8 {
    let k = slice::from_raw_parts(key as *const u8, keylen).to_vec();
    (*db).delete(&k, serial_number as u64)
}

#[no_mangle]
pub unsafe extern "C" fn f_start_session(
    db: *const FasterDb,
) {
    (*db).start_session();
}

#[no_mangle]
pub unsafe extern "C" fn f_checkpoint(
    db: *const FasterDb,
) {
    (*db).checkpoint().unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn f_refresh(
    db: *const FasterDb,
) {
    (*db).refresh();
}

#[no_mangle]
pub unsafe extern "C" fn f_stop_session(
    db: *const FasterDb,
) {
    (*db).stop_session();
}

#[no_mangle]
pub unsafe extern "C" fn f_complete_pending(
    db: *const FasterDb,
    b: bool
) {
    (*db).complete_pending(b);
}

pub struct Iter {}

/// Free an iterator.
#[no_mangle]
pub unsafe extern "C" fn f_free_iter(iter: *mut Iter) {
    drop(Box::from_raw(iter));
}

/// Iterate over all tuples.
/// Caller is responsible for freeing the returned iterator with
/// `f_free_iter`.
#[no_mangle]
pub unsafe extern "C" fn f_iter(
    db: *const FasterDb,
) -> *mut Iter {
    todo!()
    // Box::into_raw(Box::new((*db).iter()))
}

/// Get they next key from an iterator.
/// Caller is responsible for freeing the key with `f_free_buf`.
/// Returns 0 when exhausted.
#[no_mangle]
pub unsafe extern "C" fn f_iter_next_key(
    iter: *mut Iter,
    key: *mut *const c_char,
    keylen: *mut size_t,
) -> c_uchar {
    todo!()
    // match (*iter).next() {
    //     Some(Ok((k, _v))) => {
    //         *key = leak_buf(k.to_vec(), keylen);
    //         1
    //     }
    //     // TODO proper error propagation
    //     Some(Err(e)) => panic!("{:?}", e),
    //     None => 0,
    // }
} 
