use std::io::Error;
use std::path::Path;
use std::{fs::File, path::PathBuf};

use polars::prelude::CsvWriter;
use polars::prelude::*;

pub fn default_lazy_reder(path: &Path) -> LazyCsvReader {
    let p = path.to_string_lossy().to_string();

    LazyCsvReader::new(PlPath::from_string(p))
        .with_has_header(true)
        .with_separator(b';')
        .with_try_parse_dates(true)
}

pub fn default_writer(path: PathBuf) -> Result<CsvWriter<File>, Error> {
    let write_to_file = File::create(path)?;

    let writer = CsvWriter::new(write_to_file)
        .include_header(true)
        .with_separator(b';');
    Ok(writer)
}

pub fn default_file_writer(path: &mut File) -> Result<CsvWriter<&mut File>, Error> {
    let writer = CsvWriter::new(path)
        .include_header(true)
        .with_separator(b';');
    Ok(writer)
}
