use std::{fs::{self, File}, io::{self, BufRead, BufReader, ErrorKind}, path::{ PathBuf}};

use domain::conf::Conf;
use encoding_rs::WINDOWS_1250;


pub fn decode_and_remove_metadata(conf: &Conf)-> Result<PathBuf, io::Error> {
    let trade_orders_csv = &conf.trade_orders_csv;
    let tmp_directory = &conf.tmp_directory;
    
    let decoded_csv = decode_windows1250(trade_orders_csv, tmp_directory)?;
    let cleaned_csv = remove_metadata(&decoded_csv, tmp_directory)?;
    Ok(cleaned_csv)
}


fn decode_windows1250(file: &PathBuf, tmp_dir:&PathBuf) ->Result<PathBuf, io::Error> {
    let bytes = fs::read(file)?;

    let (full_input, _, malformed_content) = WINDOWS_1250.decode(&bytes);

    if malformed_content {
         return Err(io::Error::new(
                ErrorKind::InvalidInput,
                    "File {file:?} contains malformed content that cannot be decoded as Windows-1250"
    ));
    }
    let decoded_file_name = common::file::file_name_with_suffix(file.to_path_buf(),"_utf8.csv")?;
 
    let decoded_file_path = tmp_dir.join(decoded_file_name);
    fs::write(&decoded_file_path, full_input.as_bytes())?;
    Ok(decoded_file_path)
}

static HEADER: &str = "Stan;Papier;Giełda;K/S;Liczba zlecona;Liczba zrealizowana;Limit ceny;Walute;Limit aktywacji;Data zlecenia";

fn remove_metadata(trade_orders_file: &PathBuf,tmp_dir:&PathBuf) -> Result<PathBuf, io::Error> {
    let file = File::open(trade_orders_file)?;
    let reader = BufReader::new(file);
    let mut header_found = false;
    let mut csv_data_bytes: Vec<u8> = Vec::new();

    for line_result in reader.lines() {
        let line = line_result?;
        let cleaned_line2 = line.replace('\u{a0}', " ");
        let cleaned_line = cleaned_line2.trim();

        if header_found {
            csv_data_bytes.extend_from_slice(cleaned_line.as_bytes());
            csv_data_bytes.push(b'\n');
        } else if line == HEADER {
            csv_data_bytes.extend_from_slice(cleaned_line.as_bytes());
            csv_data_bytes.push(b'\n');
            header_found = true;
        }
    }
   
      let no_metadata_file = common::file::file_name_with_suffix(trade_orders_file.to_path_buf(),"_no_metadata.csv")?;
 
    let no_metadata_file_path = tmp_dir.join(no_metadata_file);
    fs::write(&no_metadata_file_path, csv_data_bytes)?;

    Ok(no_metadata_file_path)


}
