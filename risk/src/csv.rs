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
        "position_size",
        "stop_loss",
        "buy_price",
        "stop_loss_percentage",
    ])?;

    common::polars::default_writer(path)?.finish(&mut selected_col)?;

    Ok(())
}

fn prepare_lf(upside_lf: LazyFrame, conf: &Conf) -> LazyFrame {
    let commission_percent = lit(conf.commission_percent);

    upside_lf
        .clone()
        .with_columns([
            (col("buy_price") * (lit(1) - col("stop_loss_percentage")))
                .round(ROUND, ROUND_MODE)
                .alias("stop_loss"),
            (col("capital_total") * col("max_risk_percentage"))
                .round(ROUND, ROUND_MODE)
                .alias("max_risk"),
        ])
        .with_columns([((col("buy_price") - col("stop_loss"))
            + ((col("buy_price") + col("stop_loss")) * commission_percent.clone()))
        .round(ROUND, ROUND_MODE)
        .alias("risk_per_share")])
        .with_columns([(col("max_risk") / col("risk_per_share"))
            .round(ROUND, ROUND_MODE)
            .alias("position_size")])
        .with_columns([
            (col("position_size")
                * commission_percent.clone()
                * (col("buy_price") + col("stop_loss")))
            .round(ROUND, ROUND_MODE)
            .alias("stop_loss_commission"),
            (col("position_size")
                * commission_percent.clone()
                * (col("buy_price") + col("target_price")))
            .round(ROUND, ROUND_MODE)
            .alias("target_price_commission"),
        ])
        .with_columns([
            (col("position_size") * (col("buy_price") - col("stop_loss"))
                + col("stop_loss_commission"))
            .round(ROUND, ROUND_MODE)
            .alias("risk"),
            (col("position_size") * (col("target_price") - col("buy_price"))
                - col("target_price_commission"))
            .round(ROUND, ROUND_MODE)
            .alias("net_proceeds"),
        ])
        .with_columns([(col("net_proceeds") * (lit(1) - lit(0.19)))
            .round(ROUND, ROUND_MODE)
            .alias("net_profit")])
        .with_columns([(col("net_profit") / col("risk"))
            .round(ROUND, ROUND_MODE)
            .alias("reward_risk_ratio")])
        .sort(["reward_risk_ratio"], Default::default())
}
