use diesel::sqlite::SqliteConnection;
#[cfg(feature = "libsqlite3-sys")]
use libsqlite3_sys as ffi;
#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
use serde_json::{Map, Number};
use serde_json::Value;
#[cfg(feature = "sqlite-wasm-rs")]
use sqlite_wasm_rs as ffi;
#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
use std::ffi::{CStr, CString};
#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
use std::os::raw::c_char;

#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
pub fn raw_query_json(conn: &mut SqliteConnection, sql: &str) -> Result<Vec<Value>, String> {
    // SAFETY:
    // - we do not store or close the raw sqlite3*
    // - we do not touch Diesel transaction state
    // - we only prepare/step/finalize our own statement
    unsafe { conn.with_raw_connection(|db| raw_query_json_impl(db, sql)) }
}

#[cfg(not(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs")))]
pub fn raw_query_json(_conn: &mut SqliteConnection, _sql: &str) -> Result<Vec<Value>, String> {
    Err("raw sqlite query requires libsqlite3-sys or sqlite-wasm-rs feature".to_string())
}

#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
unsafe fn raw_query_json_impl(db: *mut ffi::sqlite3, sql: &str) -> Result<Vec<Value>, String> {
    let sql = CString::new(sql).map_err(|e| e.to_string())?;

    let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());

    if rc != ffi::SQLITE_OK {
        return Err(sqlite_err(db));
    }

    let mut out = Vec::new();

    loop {
        let rc = ffi::sqlite3_step(stmt);

        if rc == ffi::SQLITE_ROW {
            let col_count = ffi::sqlite3_column_count(stmt);
            let mut obj = Map::new();

            for i in 0..col_count {
                let name_ptr = ffi::sqlite3_column_name(stmt, i);
                let name = if name_ptr.is_null() {
                    format!("col_{i}")
                } else {
                    CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
                };

                let ty = ffi::sqlite3_column_type(stmt, i);
                let value = match ty {
                    ffi::SQLITE_NULL => Value::Null,

                    ffi::SQLITE_INTEGER => {
                        let x = ffi::sqlite3_column_int64(stmt, i);
                        Value::Number(Number::from(x))
                    }

                    ffi::SQLITE_FLOAT => {
                        let x = ffi::sqlite3_column_double(stmt, i);
                        match Number::from_f64(x) {
                            Some(n) => Value::Number(n),
                            None => Value::Null,
                        }
                    }

                    ffi::SQLITE_TEXT => {
                        let ptr = ffi::sqlite3_column_text(stmt, i);
                        if ptr.is_null() {
                            Value::Null
                        } else {
                            let s = CStr::from_ptr(ptr as *const c_char)
                                .to_string_lossy()
                                .into_owned();
                            Value::String(s)
                        }
                    }

                    ffi::SQLITE_BLOB => {
                        let ptr = ffi::sqlite3_column_blob(stmt, i);
                        let len = ffi::sqlite3_column_bytes(stmt, i);
                        if ptr.is_null() || len <= 0 {
                            Value::Array(vec![])
                        } else {
                            let bytes = std::slice::from_raw_parts(ptr as *const u8, len as usize);
                            Value::Array(
                                bytes
                                    .iter()
                                    .map(|b| Value::Number(Number::from(*b)))
                                    .collect(),
                            )
                        }
                    }

                    _ => Value::Null,
                };

                obj.insert(name, value);
            }

            out.push(Value::Object(obj));
        } else if rc == ffi::SQLITE_DONE {
            break;
        } else {
            ffi::sqlite3_finalize(stmt);
            return Err(sqlite_err(db));
        }
    }

    let rc = ffi::sqlite3_finalize(stmt);
    if rc != ffi::SQLITE_OK {
        return Err(sqlite_err(db));
    }

    Ok(out)
}

#[cfg(any(feature = "libsqlite3-sys", feature = "sqlite-wasm-rs"))]
unsafe fn sqlite_err(db: *mut ffi::sqlite3) -> String {
    let msg = ffi::sqlite3_errmsg(db);
    if msg.is_null() {
        "sqlite error".to_string()
    } else {
        CStr::from_ptr(msg).to_string_lossy().into_owned()
    }
}
