use std::fs::File;
use std::path::Path;

use polars::prelude::SerWriter;
use polars::{
    error::{ErrString, PolarsError},
    frame::DataFrame,
    prelude::{LazyFileListReader, LazyFrame, RoundMode, col, lit, when},
};
use std::io::Write;

pub fn with_summary(
    portfolio_csv: &Path,
    output_directory: &Path,
) -> Result<(), polars::prelude::PolarsError> {
    let net_profit_per_ticker = net_profit(portfolio_csv)?;
    let summary = summary(net_profit_per_ticker.clone())?;

    let profit_summary_path = output_directory.join("profit_summary.csv");

    let mut file = File::create(profit_summary_path)?;

    write_summary(&mut file, summary)?;
    write_net_profit_per_ticker(&mut file, net_profit_per_ticker)?;
    Ok(())
}

fn write_net_profit_per_ticker(
    output: &mut File,
    net_profit_per_ticker: LazyFrame,
) -> Result<(), polars::prelude::PolarsError> {
    let df = net_profit_per_ticker.collect()?;

    let mut selected_col = df.select([
        "ticker",
        "net_profit",
        "pct_change",
        "commission_total",
        "tax_amount",
        "buy_quantity",
        "sell_quantity",
    ])?;

    common::polars::default_file_writer(output)?.finish(&mut selected_col)?;
    Ok(())
}

fn write_summary(
    profit_summary_path: &mut File,
    summary: LazyFrame,
) -> Result<(), polars::prelude::PolarsError> {
    let res = summary.collect()?;

    let metadata = format!(
        "--- Profit Report ---\n
        Commission: {}\n
        Tax: {}\n
        Net Profit: {}\n\n",
        _money(&res, "commission_total")?,
        _money(&res, "total_tax_amount")?,
        _money(&res, "total_net_profit")?
    );

    writeln!(profit_summary_path, "{}", metadata)?;
    Ok(())
}

fn _money(df: &DataFrame, column: &str) -> Result<f64, PolarsError> {
    let val = df
        .column(column)?
        .f64()?
        .get(0)
        .ok_or(PolarsError::ComputeError(ErrString::from(format!(
            "Missing column {column} in summary"
        ))))?;
    Ok(val)
}

fn summary(net_profit: LazyFrame) -> Result<LazyFrame, polars::prelude::PolarsError> {
    let round = 2;
    let mode = RoundMode::HalfToEven;

    let summary = net_profit.clone().select([
        (col("commission_total"))
            .sum()
            .round(round, mode)
            .alias("commission_total"),
        col("tax_amount")
            .sum()
            .round(round, mode)
            .alias("total_tax_amount"),
        col("net_profit")
            .sum()
            .round(round, mode)
            .alias("total_net_profit"),
    ]);

    Ok(summary)
}
fn net_profit(portfolio_csv: &Path) -> Result<LazyFrame, polars::prelude::PolarsError> {
    let df = common::polars::default_lazy_reder(portfolio_csv).finish()?;

    let round = 2;
    let mode = RoundMode::HalfToEven;

    let tax_df = df
        .clone()
        .with_columns([
            (col("purchase_value") + col("buy_commission")).alias("cost_basis"),
            (col("sale_value") - col("sell_commission")).alias("net_proceeds"),
            (col("buy_commission") + col("sell_commission"))
                .sum()
                .round(round, mode)
                .alias("commission_total"),
        ])
        .with_column(
            (col("net_proceeds") - (col("average_cost_basis") * col("sell_quantity")))
                .round(round, mode)
                .alias("tax_base"),
        )
        .with_column(
            when(col("tax_base").gt(lit(0_f64)))
                .then(col("tax_base") * lit(0.19))
                .otherwise(lit(0_f64))
                .round(round, mode)
                .alias("tax_amount"),
        )
        .with_columns([(when(col("sell_quantity").gt(lit(0_u32)))
            .then(col("net_proceeds") - col("cost_basis") - col("tax_amount"))
            .otherwise(lit(0_f64))
            .round(round, mode)
            .alias("net_profit"))])
        .with_column(
            ((col("tax_base") / col("cost_basis")) * lit(100))
                .round(round, mode)
                .alias("pct_change"),
        );

    Ok(tax_df)
}
