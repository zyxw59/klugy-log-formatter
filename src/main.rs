use std::collections::HashMap;

use log4rs::encode::{pattern::PatternEncoder, writer::console::ConsoleWriter, Encode};
use serde::Deserialize;
use structopt::StructOpt;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Deserialize)]
struct LogLine {
  message: String,
}

#[derive(Deserialize)]
struct SerdeRecord {
  time: chrono::DateTime<chrono::FixedOffset>,
  message: String,
  module_path: Option<String>,
  file: Option<String>,
  line: Option<u32>,
  level: log::Level,
  target: String,
  thread: Option<String>,
  mdc: HashMap<String, String>,
}

fn mdc_datetime<Tz>(datetime: chrono::DateTime<Tz>)
where
  Tz: chrono::TimeZone,
{
  log_mdc::insert(
    "__log-timestamp",
    datetime
      .with_timezone(&chrono::Local)
      .format("%Y-%m-%d %H:%M:%S.%f%Z")
      .to_string(),
  );
}

#[derive(StructOpt)]
struct Args {
  /// Use kubectl output directly.
  #[structopt(short, long)]
  kubectl: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let args = Args::from_args();
  let logger = Box::new(PatternEncoder::new(
      "{h({l}):<5} {X(correlation-id):<12} {X(tenant):<30.30} {t:<20.20} {X(__log-timestamp)} - {m}{n}"
      ));
  let logger = &*Box::leak(logger);
  let stdin = tokio::io::stdin();
  let mut lines = BufReader::new(stdin).lines();
  while let Some(line) = lines.next_line().await? {
    log_line(logger, &line, args.kubectl)?;
  }
  Ok(())
}

fn log_line(logger: &'static PatternEncoder, line: &str, kubectl: bool) -> anyhow::Result<()> {
  let log_line_msg;
  let record_str = if !kubectl {
    match serde_json::from_str::<LogLine>(line) {
      Ok(log_line) => {
        log_line_msg = log_line.message;
      }
      Err(e) => {
        if !line.trim().is_empty() {
          eprintln!("Parse failure: {} in {}", e, line);
        }
        return Ok(());
      }
    }
    &log_line_msg
  } else {
    line
  };
  match serde_json::from_str::<SerdeRecord>(record_str) {
    Ok(record) => log_record(logger, record)?,
    Err(_) if !record_str.trim().is_empty() => eprintln!("{}", record_str),
    Err(_) => {}
  };
  Ok(())
}

fn log_record(logger: &'static PatternEncoder, record: SerdeRecord) -> anyhow::Result<()> {
  let mut thread_builder = std::thread::Builder::new();
  if let Some(thread) = &record.thread {
    thread_builder = thread_builder.name(thread.clone());
  }
  thread_builder
    .spawn(move || {
      log_mdc::extend(record.mdc.iter());
      mdc_datetime(record.time);
      let message = &record.message;
      if let Some(mut console) = ConsoleWriter::stdout() {
        logger
          .encode(
            &mut console,
            &log::Record::builder()
              .args(format_args!("{}", message))
              .module_path(record.module_path.as_deref())
              .file(record.file.as_deref())
              .line(record.line)
              .level(record.level)
              .target(&record.target)
              .build(),
          )
          .unwrap();
      }
    })?
    .join()
    .unwrap();
  Ok(())
}
