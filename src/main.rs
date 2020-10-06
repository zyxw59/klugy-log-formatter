use log4rs::encode::{pattern::PatternEncoder, writer::console::ConsoleWriter, Encode};
use serde::Deserialize;
use std::collections::HashMap;

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
    datetime.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S.%f%Z").to_string(),
  );
}

fn main() -> anyhow::Result<()> {
  let logger = Box::new(PatternEncoder::new(
      "{h({l}):<5} {X(correlation-id):<12} {X(tenant):<30.30} {t:<20.20} {X(__log-timestamp)} - {m}{n}"
      ));
  let logger = &*Box::leak(logger);
  let stdin = std::io::stdin();
  let mut line = String::new();
  loop {
    stdin.read_line(&mut line)?;
    match serde_json::from_str::<LogLine>(&line) {
      Ok(log_line) => match serde_json::from_str::<SerdeRecord>(&log_line.message) {
        Ok(record) => {
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
        }
        Err(_) if !log_line.message.trim().is_empty() => eprintln!("{}", log_line.message),
        Err(_) => {}
      },
      Err(e) if !line.trim().is_empty() => eprintln!("Parse failure: {} in {}", e, line),
      Err(_) => {}
    }
    line.clear();
  }
}
