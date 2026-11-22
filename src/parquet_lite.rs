//! A minimal Parquet writer for the two output tables: one row group, one
//! uncompressed data page (v1, PLAIN) per column, Thrift compact metadata.
//! INT32 DATE, INT64 and UTF8 BYTE_ARRAY columns, REQUIRED or OPTIONAL.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

/// Thrift compact protocol, just what Parquet's metadata needs.
struct Thrift {
    out: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift {
            out: Vec::new(),
            last: vec![0],
        }
    }

    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.out.push((value as u8) | 0x80);
            value >>= 7;
        }
        self.out.push(value as u8);
    }

    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }

    fn field(&mut self, id: i16, kind: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | kind);
        } else {
            self.out.push(kind);
            self.zigzag(i64::from(id));
        }
        *self.last.last_mut().unwrap() = id;
    }

    fn i32(&mut self, id: i16, value: i32) {
        self.field(id, T_I32);
        self.zigzag(i64::from(value));
    }

    fn i64(&mut self, id: i16, value: i64) {
        self.field(id, T_I64);
        self.zigzag(value);
    }

    fn binary(&mut self, id: i16, value: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(value.len() as u64);
        self.out.extend_from_slice(value);
    }

    fn begin_struct(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }

    /// A struct as a list element (no field header).
    fn begin_element(&mut self) {
        self.last.push(0);
    }

    fn end_struct(&mut self) {
        self.out.push(0);
        self.last.pop();
    }

    fn list(&mut self, id: i16, kind: u8, len: usize) {
        self.field(id, T_LIST);
        if len < 15 {
            self.out.push(((len as u8) << 4) | kind);
        } else {
            self.out.push(0xF0 | kind);
            self.varint(len as u64);
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Date,
    Int64,
    Text,
}

/// One column's encoded page values, and for an OPTIONAL column which rows
/// are present.
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub values: Vec<u8>,
    pub present: Option<Vec<bool>>,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, optional: bool, rows: usize) -> Self {
        let width = match kind {
            Kind::Date => 4,
            Kind::Int64 => 8,
            Kind::Text => 12,
        };
        Column {
            name,
            kind,
            values: Vec::with_capacity(rows * width),
            present: optional.then(|| Vec::with_capacity(rows)),
        }
    }

    #[inline]
    pub fn date(&mut self, days: i32) {
        self.values.extend_from_slice(&days.to_le_bytes());
    }

    #[inline]
    pub fn int64(&mut self, value: i64) {
        self.values.extend_from_slice(&value.to_le_bytes());
    }

    /// A text value; for an OPTIONAL column, None is a null.
    #[inline]
    pub fn text(&mut self, value: Option<&[u8]>) {
        if let Some(present) = &mut self.present {
            present.push(value.is_some());
        }
        if let Some(value) = value {
            self.values.extend_from_slice(&(value.len() as u32).to_le_bytes());
            self.values.extend_from_slice(value);
        }
    }

    /// Appends a text value written by `write` (for a REQUIRED column).
    #[inline]
    pub fn text_with(&mut self, write: impl FnOnce(&mut Vec<u8>)) {
        let at = self.values.len();
        self.values.extend_from_slice(&[0; 4]);
        write(&mut self.values);
        let len = (self.values.len() - at - 4) as u32;
        self.values[at..at + 4].copy_from_slice(&len.to_le_bytes());
    }
}

/// Definition levels (bit width 1) as one bit-packed run, with the 4-byte
/// length prefix of a v1 data page.
fn definition_levels(present: &[bool]) -> Vec<u8> {
    let groups = present.len().div_ceil(8);
    let mut run = Thrift::new();
    run.varint(((groups as u64) << 1) | 1);
    let mut bytes = run.out;
    for group in present.chunks(8) {
        let mut byte = 0u8;
        for (at, &here) in group.iter().enumerate() {
            byte |= u8::from(here) << at;
        }
        bytes.push(byte);
    }
    let mut out = (bytes.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&bytes);
    out
}

/// Writes `columns` (all with `rows` rows) as `dir/file`.
pub fn write(dir: &Path, file: &str, rows: usize, columns: &[Column]) {
    fs::create_dir_all(dir).expect("create output folder");
    let mut out = BufWriter::with_capacity(1 << 22, File::create(dir.join(file)).expect("create parquet file"));
    out.write_all(b"PAR1").unwrap();
    let mut offset = 4u64;
    // (data page offset, total size) per column.
    let mut chunks = Vec::with_capacity(columns.len());
    for column in columns {
        let levels = column.present.as_deref().map(definition_levels).unwrap_or_default();
        let page_size = levels.len() + column.values.len();
        let mut header = Thrift::new();
        header.i32(1, 0); // DATA_PAGE
        header.i32(2, page_size as i32);
        header.i32(3, page_size as i32);
        header.begin_struct(5);
        header.i32(1, rows as i32);
        header.i32(2, 0); // PLAIN
        header.i32(3, 3); // RLE
        header.i32(4, 3); // RLE
        header.end_struct();
        header.out.push(0);
        out.write_all(&header.out).unwrap();
        out.write_all(&levels).unwrap();
        out.write_all(&column.values).unwrap();
        let total = (header.out.len() + page_size) as u64;
        chunks.push((offset, total));
        offset += total;
    }

    let physical = |kind: Kind| match kind {
        Kind::Date => 1,  // INT32
        Kind::Int64 => 2, // INT64
        Kind::Text => 6,  // BYTE_ARRAY
    };
    let mut meta = Thrift::new();
    meta.i32(1, 1);
    meta.list(2, T_STRUCT, columns.len() + 1);
    meta.begin_element();
    meta.binary(4, b"schema");
    meta.i32(5, columns.len() as i32);
    meta.end_struct();
    for column in columns {
        meta.begin_element();
        meta.i32(1, physical(column.kind));
        meta.i32(3, i32::from(column.present.is_some())); // REQUIRED 0, OPTIONAL 1
        meta.binary(4, column.name.as_bytes());
        match column.kind {
            Kind::Date => {
                meta.i32(6, 6); // converted type DATE
                meta.begin_struct(10);
                meta.begin_struct(6); // LogicalType.DATE
                meta.end_struct();
                meta.end_struct();
            }
            Kind::Text => {
                meta.i32(6, 0); // converted type UTF8
                meta.begin_struct(10);
                meta.begin_struct(1); // LogicalType.STRING
                meta.end_struct();
                meta.end_struct();
            }
            Kind::Int64 => {}
        }
        meta.end_struct();
    }
    meta.i64(3, rows as i64);
    meta.list(4, T_STRUCT, 1);
    meta.begin_element();
    meta.list(1, T_STRUCT, columns.len());
    let mut total_size = 0i64;
    for (column, &(page_offset, size)) in columns.iter().zip(&chunks) {
        meta.begin_element();
        meta.i64(2, page_offset as i64);
        meta.begin_struct(3);
        meta.i32(1, physical(column.kind));
        meta.list(2, T_I32, 2);
        meta.zigzag(0); // PLAIN
        meta.zigzag(3); // RLE
        meta.list(3, T_BINARY, 1);
        meta.varint(column.name.len() as u64);
        meta.out.extend_from_slice(column.name.as_bytes());
        meta.i32(4, 0); // UNCOMPRESSED
        meta.i64(5, rows as i64);
        meta.i64(6, size as i64);
        meta.i64(7, size as i64);
        meta.i64(9, page_offset as i64);
        meta.end_struct();
        meta.end_struct();
        total_size += size as i64;
    }
    meta.i64(2, total_size);
    meta.i64(3, rows as i64);
    meta.end_struct();
    meta.binary(6, b"shopping-sessions");
    meta.out.push(0);
    out.write_all(&meta.out).unwrap();
    out.write_all(&(meta.out.len() as u32).to_le_bytes()).unwrap();
    out.write_all(b"PAR1").unwrap();
    out.flush().unwrap();
}
