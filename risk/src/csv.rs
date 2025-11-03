use domain::conf::Conf;
use domain::constant::{ROUND, ROUND_MODE};
use polars::prelude::LazyFileListReader;
use polars::prelude::SerWriter;
use polars::prelude::{LazyFrame, col, lit};

pub fn calculate(conf: &Conf) -> Result<(), polars::prelude::PolarsError> {
    let upside_lf = common::polars::default_lazy_reder(conf.upside_csv.as_path()).finish()?;

    let path = conf.outpu_directory.join("risk.csv");

    let risk_lf = &mut prepare_lf(upside_lf, conf).collect()?;

    let mut selected_col = risk_lf.select([
        "ticker",
        "reward_risk_ratio",
        "net_profit",
        "max_quantity",
        "stop_loss",
        "buy_price",
        "percentage_stop_loss",
    ])?;

    common::polars::default_writer(path)?.finish(&mut selected_col)?;

    Ok(())
}

fn prepare_lf(upside_lf: LazyFrame, conf: &Conf) -> LazyFrame {
    let commission_percent = lit(conf.commission_percent);

    upside_lf
        .clone()
        .with_columns([
            (col("buy_price") * (lit(1) - col("percentage_stop_loss")))
                .round(ROUND, ROUND_MODE)
                .alias("stop_loss"),
            (col("capital_total") * col("max_risk_percentage_per_transaction"))
                .round(ROUND, ROUND_MODE)
                .alias("max_acceptable_risk"),
        ])
        .with_columns([((col("buy_price") - col("stop_loss"))
            + (col("buy_price") * commission_percent.clone())
            + (col("stop_loss") * commission_percent.clone()))
        .round(ROUND, ROUND_MODE)
        .alias("risk_per_share")])
        .with_columns([(col("max_acceptable_risk") / col("risk_per_share"))
            .round(ROUND, ROUND_MODE)
            .alias("max_quantity")])
        .with_columns([
            (col("max_quantity")
                * commission_percent.clone()
                * (col("buy_price") + col("stop_loss")))
            .round(ROUND, ROUND_MODE)
            .alias("stop_loss_commission"),
            (col("max_quantity") * commission_percent.clone() * (col("buy_price") + col("upside")))
                .round(ROUND, ROUND_MODE)
                .alias("upside_commission"),
        ])
        .with_columns([
            (col("max_quantity") * (col("buy_price") - col("stop_loss"))
                + col("stop_loss_commission"))
            .round(ROUND, ROUND_MODE)
            .alias("risk"),
            (col("max_quantity") * (col("upside") - col("buy_price")) - col("upside_commission"))
                .round(ROUND, ROUND_MODE)
                .alias("net_proceeds"),
        ])
        .with_columns([(col("net_proceeds") * (lit(1) - lit(0.19)))
            .round(ROUND, ROUND_MODE)
            .alias("net_profit")])
        .with_columns([(col("net_profit") / col("risk"))
            .round(ROUND, ROUND_MODE)
            .alias("reward_risk_ratio")])
}
