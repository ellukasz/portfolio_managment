use std::path::Path;

use domain::conf::Conf;

fn main() {
    let trade_orders_csv =  Path::new("C:\\_lukasz\\rust\\data\\2025-10-21.csv").to_path_buf();
    let tmp_directory = Path::new("C:\\_lukasz\\rust\\data\\tmp").to_path_buf();
    let outpu_directory = Path::new("c:\\_lukasz\\rust\\data\\out").to_path_buf();
    let commission_percent = 0.039;
    let commission_min = 5.0;
    let conf = Conf {
        trade_orders_csv,
        outpu_directory,
        tmp_directory,
        commission_percent,
        commission_min,
    };

    let filed_trade_orders_csv=  mbank_csv::csv::write(&conf).unwrap();
    profit::profit::with_summary(&filed_trade_orders_csv,&conf.outpu_directory).unwrap();
}
