use domain::conf::Conf;
use domain::constant::{ROUND, ROUND_MODE};
use polars::chunked_array::ops::SortMultipleOptions;
use polars::prelude::LazyFileListReader;
use polars::prelude::SerWriter;
use polars::prelude::when;
use polars::prelude::{LazyFrame, col, lit};

pub fn calculate(conf: &Conf) -> Result<(), polars::prelude::PolarsError> {
    let upside_lf = common::polars::default_lazy_reder(conf.upside_csv.as_path()).finish()?;

    let path = conf.outpu_directory.join("risk.csv");

    let risk_lf = &mut prepare_lf(upside_lf, conf).collect()?;

    let mut selected_col = risk_lf.select([
        "ticker",
        "reward_risk_ratio",
        "target_price",
        "buy_price",
        "net_profit",
        "position_size",
        "stop_loss",
        "stop_loss_percentage",
        "capital_utilization_pct",
    ])?;

    common::polars::default_writer(path)?.finish(&mut selected_col)?;

    Ok(())
}

fn prepare_lf(upside_lf: LazyFrame, conf: &Conf) -> LazyFrame {
    let commission_percent = lit(conf.commission_percent);
    let belka_tax = lit(0.19);

    upside_lf
        .with_columns([
            (col("buy_price") * (lit(1.0) - col("stop_loss_percentage"))).alias("stop_loss"),
            // maksymalna kwota do stracenia (Cash at Risk)
            (col("capital_total") * col("max_risk_percentage")).alias("max_risk_cash"),
        ])
        .with_columns([
            // ryzyko na 1 akcję (różnica kursowa + prowizja kupna i sprzedaży)
            ((col("buy_price") - col("stop_loss"))
                + ((col("buy_price") + col("stop_loss")) * commission_percent.clone()))
            .alias("unit_risk"),
        ])
        .with_columns([
            // Rozmiar pozycji (zaokrąglony w dół do liczb całkowitych)
            (col("max_risk_cash") / col("unit_risk"))
                .floor()
                .alias("position_size"),
        ])
        .with_columns([
            //  Koszty operacyjne dla SL i Target
            (col("position_size")
                * commission_percent.clone()
                * (col("buy_price") + col("stop_loss")))
            .alias("sl_commission_total"),
            (col("position_size")
                * commission_percent.clone()
                * (col("buy_price") + col("target_price")))
            .alias("tp_commission_total"),
        ])
        .with_columns([
            //  Realne ryzyko (strata na kursie + prowizje)
            (col("position_size") * (col("buy_price") - col("stop_loss"))
                + col("sl_commission_total"))
            .alias("total_risk_amount"),
            //  Zysk brutto (różnica kursowa - prowizje)
            (col("position_size") * (col("target_price") - col("buy_price"))
                - col("tp_commission_total"))
            .alias("net_proceeds"),
        ])
        .with_columns([
            //  Podatek Belki (naliczany tylko od zysku!)
            when(col("net_proceeds").gt(0.0))
                .then(col("net_proceeds") * (lit(1.0) - belka_tax))
                .otherwise(col("net_proceeds"))
                .alias("net_profit"),
        ])
        .with_columns([
            //Reward/Risk
            (col("net_profit") / col("total_risk_amount")).alias("reward_risk_ratio"),
        ])
        .with_columns([
            ((col("position_size") * col("buy_price")) / col("capital_total") * lit(100.0))
                .alias("capital_utilization_pct"),
        ])
        .with_columns([
            col("net_profit").round(ROUND, ROUND_MODE),
            col("reward_risk_ratio").round(2, ROUND_MODE),
            col("total_risk_amount").round(ROUND, ROUND_MODE),
            col("capital_utilization_pct").round(ROUND, ROUND_MODE),
            col("stop_loss").round(ROUND, ROUND_MODE),
        ])
        .sort(
            ["reward_risk_ratio"],
            SortMultipleOptions {
                descending: vec![true], // Wektor booli dla każdej z kolumn
                ..Default::default()
            },
        )
}
