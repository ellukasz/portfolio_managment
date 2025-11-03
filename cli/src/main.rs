use std::path::Path;

use domain::conf::Conf;

fn main() {
    let trade_orders_csv = Path::new("..\\data\\trade_orders.csv").to_path_buf();
    let tmp_directory = Path::new("..\\data\\tmp").to_path_buf();
    let outpu_directory = Path::new("..\\data\\out").to_path_buf();

    let commission_percent = 0.039;
    let commission_min = 5.0;

    let upside_csv = Path::new("c:\\_lukasz\\rust\\data\\upside.csv").to_path_buf();

    let conf = Conf {
        trade_orders_csv,
        outpu_directory,
        tmp_directory,
        commission_percent,
        commission_min,
        upside_csv,
    };

    let filed_trade_orders_csv = mbank_csv::csv::write(&conf).unwrap();
    profit::csv::profit_with_summary(&filed_trade_orders_csv, &conf).unwrap();

    risk::csv::calculate(&conf).unwrap();
}
