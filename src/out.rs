//! Writes the two output tables as uncompressed Parquet.

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

pub struct CampaignRow {
    pub campaign_id: Vec<u8>,
    pub channel: Vec<u8>,
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assisted_sessions: i64,
}

fn properties() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .set_max_row_group_row_count(Some(usize::MAX))
            .build(),
    )
}

type Writer = SerializedFileWriter<BufWriter<File>>;

fn open(dir: &Path, part: usize, schema: &str) -> Writer {
    fs::create_dir_all(dir).expect("create output folder");
    let file = File::create(dir.join(format!("part-{part}.parquet"))).expect("create parquet file");
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    SerializedFileWriter::new(
        BufWriter::with_capacity(1 << 22, file),
        schema,
        properties(),
    )
    .expect("parquet writer")
}

fn bytes(values: impl Iterator<Item = Vec<u8>>) -> Vec<ByteArray> {
    values.map(ByteArray::from).collect()
}

macro_rules! column {
    ($group:expr, $kind:ty, $values:expr, $levels:expr) => {{
        let mut column = $group.next_column().expect("column").expect("column");
        column
            .typed::<$kind>()
            .write_batch(&$values, $levels, None)
            .expect("write column");
        column.close().expect("close column");
    }};
}

/// One user_daily row: (person, day, sessions, events, revenue, category),
/// with category `no_category` for none.
pub type UserRow = (u64, i32, u32, u32, u64, u16);

/// Writes one user_daily file. Every user ID goes into one shared buffer and
/// each value is a slice of it, so no row allocates.
/// Writes one user_daily file with the minimal writer (the default) or, with
/// SESSIONS_WRITER=crate, the parquet crate.
pub fn write_users(
    dir: &Path,
    part: usize,
    rows: &[UserRow],
    write_name: impl Fn(u64, &mut Vec<u8>),
    categories: &[&[u8]],
    no_category: u16,
) {
    if std::env::var("SESSIONS_WRITER").as_deref() == Ok("crate") {
        return write_users_crate(dir, part, rows, write_name, categories, no_category);
    }
    use crate::parquet_lite::{Column, Kind, write};
    let n = rows.len();
    let mut user_id = Column::new("user_id", Kind::Text, false, n);
    let mut day = Column::new("day", Kind::Date, false, n);
    let mut sessions = Column::new("sessions", Kind::Int64, false, n);
    let mut events = Column::new("events", Kind::Int64, false, n);
    let mut revenue = Column::new("revenue_cents", Kind::Int64, false, n);
    let mut top = Column::new("top_category", Kind::Text, true, n);
    for row in rows {
        user_id.text_with(|out| write_name(row.0, out));
        day.date(row.1);
        sessions.int64(i64::from(row.2));
        events.int64(i64::from(row.3));
        revenue.int64(row.4 as i64);
        top.text((row.5 != no_category).then(|| categories[row.5 as usize]));
    }
    let columns = [user_id, day, sessions, events, revenue, top];
    write(dir, &format!("part-{part}.parquet"), n, &columns);
    std::mem::forget(columns);
}

fn write_users_crate(
    dir: &Path,
    part: usize,
    rows: &[UserRow],
    write_name: impl Fn(u64, &mut Vec<u8>),
    categories: &[&[u8]],
    no_category: u16,
) {
    let mut writer = open(
        dir,
        part,
        "message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }",
    );
    let mut group = writer.next_row_group().expect("row group");
    let mut buffer = Vec::with_capacity(rows.len() * 12);
    let mut ends = Vec::with_capacity(rows.len());
    for row in rows {
        write_name(row.0, &mut buffer);
        ends.push(buffer.len());
    }
    let shared = bytes::Bytes::from(buffer);
    let mut start = 0;
    let ids: Vec<ByteArray> = ends
        .iter()
        .map(|&end| {
            let value = ByteArray::from(shared.slice(start..end));
            start = end;
            value
        })
        .collect();
    column!(group, ByteArrayType, ids, None);
    let days: Vec<i32> = rows.iter().map(|r| r.1).collect();
    column!(group, Int32Type, days, None);
    let sessions: Vec<i64> = rows.iter().map(|r| i64::from(r.2)).collect();
    column!(group, Int64Type, sessions, None);
    let events: Vec<i64> = rows.iter().map(|r| i64::from(r.3)).collect();
    column!(group, Int64Type, events, None);
    let revenue: Vec<i64> = rows.iter().map(|r| r.4 as i64).collect();
    column!(group, Int64Type, revenue, None);
    let levels: Vec<i16> = rows.iter().map(|r| i16::from(r.5 != no_category)).collect();
    let names: Vec<ByteArray> = categories
        .iter()
        .map(|name| ByteArray::from(bytes::Bytes::copy_from_slice(name)))
        .collect();
    let values: Vec<ByteArray> = rows
        .iter()
        .filter(|r| r.5 != no_category)
        .map(|r| names[r.5 as usize].clone())
        .collect();
    column!(group, ByteArrayType, values, Some(&levels[..]));
    group.close().expect("close row group");
    writer.close().expect("close parquet");
    // Freeing each value one by one is wasted work: the process exits soon.
    std::mem::forget((ids, values));
}

pub fn write_campaigns(dir: &Path, rows: &[CampaignRow]) {
    if std::env::var("SESSIONS_WRITER").as_deref() == Ok("crate") {
        return write_campaigns_crate(dir, rows);
    }
    use crate::parquet_lite::{Column, Kind, write};
    let n = rows.len();
    let mut campaign_id = Column::new("campaign_id", Kind::Text, false, n);
    let mut channel = Column::new("channel", Kind::Text, false, n);
    let mut day = Column::new("day", Kind::Date, false, n);
    let mut sessions = Column::new("sessions", Kind::Int64, false, n);
    let mut purchasing = Column::new("purchasing_sessions", Kind::Int64, false, n);
    let mut revenue = Column::new("revenue_cents", Kind::Int64, false, n);
    let mut assisted = Column::new("assisted_sessions", Kind::Int64, false, n);
    for row in rows {
        campaign_id.text(Some(&row.campaign_id));
        channel.text(Some(&row.channel));
        day.date(row.day);
        sessions.int64(row.sessions);
        purchasing.int64(row.purchasing_sessions);
        revenue.int64(row.revenue);
        assisted.int64(row.assisted_sessions);
    }
    let columns = [campaign_id, channel, day, sessions, purchasing, revenue, assisted];
    write(dir, "part-0.parquet", n, &columns);
}

fn write_campaigns_crate(dir: &Path, rows: &[CampaignRow]) {
    let mut writer = open(
        dir,
        0,
        "message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }",
    );
    let mut group = writer.next_row_group().expect("row group");
    let ids = bytes(rows.iter().map(|r| r.campaign_id.clone()));
    column!(group, ByteArrayType, ids, None);
    let channels = bytes(rows.iter().map(|r| r.channel.clone()));
    column!(group, ByteArrayType, channels, None);
    let days: Vec<i32> = rows.iter().map(|r| r.day).collect();
    column!(group, Int32Type, days, None);
    let sessions: Vec<i64> = rows.iter().map(|r| r.sessions).collect();
    column!(group, Int64Type, sessions, None);
    let purchasing: Vec<i64> = rows.iter().map(|r| r.purchasing_sessions).collect();
    column!(group, Int64Type, purchasing, None);
    let revenue: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
    column!(group, Int64Type, revenue, None);
    let assisted: Vec<i64> = rows.iter().map(|r| r.assisted_sessions).collect();
    column!(group, Int64Type, assisted, None);
    group.close().expect("close row group");
    writer.close().expect("close parquet");
}
