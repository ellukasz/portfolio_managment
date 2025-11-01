use std::path::PathBuf;

use domain::conf::Conf;
use polars::{io::SerWriter, prelude::{LazyFileListReader, LazyFrame, RoundMode, col, lit, when}};

use crate::preprocess_csv;

const ROUND: u32 = 2;
const ROUND_MODE:RoundMode = RoundMode::HalfToEven;

pub fn write(conf: &Conf) -> Result<PathBuf, polars::prelude::PolarsError> {
    let trade_orders_csv = preprocess_csv::decode_and_remove_metadata(&conf)?;
    let df = normalize_csv(&trade_orders_csv, &conf)?;

    let trade_orders_normalized_file = common::file::file_name_with_suffix(
        trade_orders_csv,
        "normalized.csv",
    )?;
    let trade_orders_normalized_path = conf.tmp_directory.join(trade_orders_normalized_file);
    common::polars::default_writer( trade_orders_normalized_path.clone())?
    .finish(&mut df.collect()?)?;

    Ok(trade_orders_normalized_path)
}

fn normalize_csv(csv: &PathBuf,conf:&Conf) ->Result<LazyFrame, polars::prelude::PolarsError> {
    let commission_percent = lit(conf.commission_percent);
    let commission_min = lit(conf.commission_min);
    print!("{}",&commission_percent);
    let df = common::polars::default_lazy_reder(csv)
    .with_decimal_comma(true)
    .finish()?;

    let dataset= df
    .filter(col("Stan").eq(lit("Zrealizowane")))
    .group_by([col("Papier").alias("ticker")])
    .agg([
        when(col("K/S").eq(lit("K")))
        .then(col("Liczba zrealizowana"))
        .otherwise(lit(0_u32))
        .sum()
        .alias("buy_quantity"),

        when(col("K/S").eq(lit("S")))
        .then(col("Liczba zrealizowana"))
        .otherwise(lit(0_u32))
        .sum()
        .alias("sell_quantity"),
        
        when(col("K/S").eq(lit("K")))
        .then(col("Limit ceny") * col("Liczba zrealizowana"))
        .otherwise(lit(0_f64))
        .sum()
        .round(ROUND, ROUND_MODE)
        .alias("purchase_value"),

        when(col("K/S").eq(lit("S")))
        .then(col("Limit ceny") * col("Liczba zrealizowana"))
        .otherwise(lit(0_f64))
        .sum()
        .round(ROUND, ROUND_MODE)
        .alias("sale_value"),
    ])
    .with_columns([
        when((col("purchase_value")*commission_percent.clone())
        .gt(commission_min.clone()))
       .then(col("purchase_value")*commission_percent.clone()) 
       .otherwise(commission_min.clone())
        .round(ROUND, ROUND_MODE)
        .alias("buy_commision"),

        when(
        (col("sale_value")*commission_percent.clone()).gt(commission_min.clone())
        )
       .then(col("sale_value")*commission_percent.clone()) 
       .otherwise(
        when(col("sell_quantity").gt(lit(0_u32)))
        .then(commission_min.clone())
        .otherwise(0_f64)
        )
        .round(ROUND, ROUND_MODE)
        .alias("sell_commision"),
    ])
    .with_columns([
        ((col("purchase_value") + col("buy_commision"))/col("buy_quantity"))
        .round(ROUND, ROUND_MODE)
        .alias("average_cost_basis"),
    ])
     .sort(
            ["ticker"],Default::default(),
        );
    
    Ok(dataset)
}