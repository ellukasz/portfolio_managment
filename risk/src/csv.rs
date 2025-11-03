use domain::conf::Conf;
use domain::constant::{ROUND, ROUND_MODE};
use polars::prelude::LazyFileListReader;
use polars::prelude::SerWriter;
use polars::prelude::{LazyFrame, col, lit};

pub fn calculate(conf: &Conf) -> Result<(), polars::prelude::PolarsError> {
    let upside_lf = common::polars::default_lazy_reder(conf.upside_csv.as_path()).finish()?;

    let path = conf.outpu_directory.join("risk.csv");

    let risk_lf = &mut prepare_lf(upside_lf, conf).collect()?;

    //    common::polars::default_writer(path)?.finish( risk_lf)?;

    let mut selected_col = risk_lf.select([
        "ticker",
        "reward_risk_ratio",
        "net_profit",
        "max_quantity",
        "stop_loss",
        "current_price",
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
            (col("current_price") * (lit(1) - col("percentage_stop_loss")))
                .round(ROUND, ROUND_MODE)
                .alias("stop_loss"),
            (col("capital_total") * col("max_risk_percentage_per_transaction"))
                .round(ROUND, ROUND_MODE)
                .alias("max_acceptable_risk"),
        ])
        .with_columns([((col("current_price") - col("stop_loss"))
            + (col("current_price") * commission_percent.clone())
            + (col("stop_loss") * commission_percent.clone()))
        .round(ROUND, ROUND_MODE)
        .alias("risk_per_share")])
        .with_columns([(col("max_acceptable_risk") / col("risk_per_share"))
            .round(ROUND, ROUND_MODE)
            .alias("max_quantity")])
        .with_columns([
            (col("max_quantity")
                * commission_percent.clone()
                * (col("current_price") + col("stop_loss")))
            .round(ROUND, ROUND_MODE)
            .alias("stop_loss_commission"),
            (col("max_quantity")
                * commission_percent.clone()
                * (col("current_price") + col("upside")))
            .round(ROUND, ROUND_MODE)
            .alias("upside_commission"),
        ])
        .with_columns([
            (col("max_quantity") * (col("current_price") - col("stop_loss"))
                + col("stop_loss_commission"))
            .round(ROUND, ROUND_MODE)
            .alias("risk"),
            (col("max_quantity") * (col("upside") - col("current_price"))
                - col("upside_commission"))
            .round(ROUND, ROUND_MODE)
            .alias("net_proceeds"),
        ])
        .with_columns([(col("net_proceeds") * (lit(1) - lit(0.19)))
            .round(ROUND, ROUND_MODE)
            .alias("net_profit")])
        .with_columns([(col("net_profit") / col("risk"))
            .round(ROUND, ROUND_MODE)
            .alias("reward_risk_ratio")])

    /*
        upside_lf
        .clone()
        .with_columns([
            (col("capital_total")*col("max_risk_percentage_per_transaction"))
            .round(ROUND,ROUND_MODE)
            .alias("risk_amount"),

            (col("current_price")*(lit(1)-col("percentage_stop_loss")))
            .round(ROUND,ROUND_MODE)
            .alias("stop_loss"),
        ]).with_columns([
            (col("current_price")-col("stop_loss"))
            .round(ROUND,ROUND_MODE)
            .alias("risk_per_share"),
        ]).with_columns([
            (col("risk_amount")/col("risk_per_share"))
            .round(1_u32,ROUND_MODE)
            .alias("max_quantity"),
        ]).with_columns([

            (col("max_quantity")*col("current_price"))
            .round(ROUND,ROUND_MODE)
            .alias("purchase_value"),

            (col("max_quantity")*col("upside"))
            .round(ROUND,ROUND_MODE)
            .alias("sale_value"),
        ])
        .with_columns([
                when((col("purchase_value") * commission_percent.clone()).gt(commission_min.clone()))
                    .then(col("purchase_value") * commission_percent.clone())
                    .otherwise(commission_min.clone())
                    .round(ROUND, ROUND_MODE)
                    .alias("buy_commission"),
                when((col("sale_value") * commission_percent.clone()).gt(commission_min.clone()))
                    .then(col("sale_value") * commission_percent.clone())
                    .otherwise(commission_min.clone())
                    .round(ROUND, ROUND_MODE)
                    .alias("sell_commission"),
            ])
                    .with_columns([
                (col("purchase_value") + col("buy_commission")).alias("cost_basis"),
                (col("sale_value") - col("sell_commission")).alias("net_proceeds"),
            ])
            .with_column(
                (col("net_proceeds") - col("cost_basis") )
                    .round(ROUND, ROUND_MODE)
                    .alias("tax_base"),
            ).with_column(
                    (col("tax_base") * lit(0.19))
                    .round(ROUND, ROUND_MODE)
                    .alias("tax_amount"),
            ).with_column((col("net_proceeds") - col("cost_basis") - col("tax_amount"))
                .round(ROUND, ROUND_MODE)
                .alias("potential_profit"))
                .with_column((
                    col("")
                ))

    */

    /*
    .with_columns([

        (col("investment_amount")*col("risk"))
        .round(ROUND,ROUND_MODE)
        .alias("max_acceptable_loss"),

        (col("current_price")*(lit(1)-col("stop_loss_percent")))
        .round(ROUND,ROUND_MODE)
        .alias("stop_loss")

    ]).with_columns([
        (col("upside")-col("current_price"))/(col("current_price")-col("stop_loss"))
        .round(ROUND,ROUND_MODE)
        .alias("reward_risk_ratio"),

        (col("current_price")-col("stop_loss"))
        .alias("loss_per_share")
    ])
    .with_column(
        (col("max_acceptable_loss")/col("loss_per_share"))
        .round(1_u32,ROUND_MODE)
        .alias("max_quantity")
    )
    */
}
