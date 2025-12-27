use domain::conf::Conf;
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.is_empty() {
        eprintln!("Usage: clie.exe <input_dir> ");
        std::process::exit(1);
    }

    let inpuit_dir = &args[1];
    eprintln!("Input dir: {}", inpuit_dir);

    let tmp_directory = Path::new(inpuit_dir).join("tmp");
    if let Err(e) = fs::create_dir_all(&tmp_directory) {
        eprintln!("Error creating tmp directory: {}", e);
        std::process::exit(1);
    }
    eprintln!("Tmp dir: {:?}", tmp_directory);

    let outpu_directory = Path::new(inpuit_dir).join("out");
    if let Err(e) = fs::create_dir_all(&outpu_directory) {
        eprintln!("Error creating out directory: {}", e);
        std::process::exit(1);
    }

    eprintln!("Out dir: {:?}", outpu_directory);

    let trade_orders_csv = Path::new(inpuit_dir).join("trade_orders.csv").to_path_buf();

    let commission_percent = 0.039;
    let commission_min = 5.0;

    let upside_csv = Path::new(inpuit_dir).join("upside.csv").to_path_buf();

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
