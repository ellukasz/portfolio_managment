use std::{
    io::{self, ErrorKind},
    path::{ PathBuf},
};

pub fn file_name_with_suffix(base_file: PathBuf, suffix: &str) -> Result<String, io::Error> {

    let file_name = base_file
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                "Invalid file name: could not get valid filename stem",
            )
        })?;

    let new_file_name = format!("{file_name}_{suffix}");

    Ok(new_file_name)
}
