use std::path::PathBuf;

pub struct Conf {
    pub trade_orders_csv: PathBuf,
    pub outpu_directory: PathBuf,
    pub tmp_directory: PathBuf,
    pub commission_percent: f64,
    pub commission_min: f64,
    pub upside_csv: PathBuf,
}
