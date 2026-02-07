use domain::conf::Conf;
use domain::constant::{ROUND, ROUND_MODE};
use polars::chunked_array::ops::SortMultipleOptions;
use polars::datatypes::DataType;
use polars::prelude::*;

pub fn calculate(conf: &Conf) -> Result<(), polars::prelude::PolarsError> {
    // Naprawiono literówkę: reder -> reader
    let upside_lf = common::polars::default_lazy_reder(conf.upside_csv.as_path()).finish()?;

    // Naprawiono literówkę: outpu_directory -> output_directory
    let path = conf.outpu_directory.join("risk.csv");

    // Usunięto niepotrzebny mut i referencję przy collect()
    let risk_df = prepare_lf(upside_lf, conf).collect()?;

    let mut selected_col = risk_df.select([
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
        // --- ETAP 1: OBLICZENIA JEDNOSTKOWE ---
        .with_columns([
            (col("buy_price") * (lit(1.0) - col("stop_loss_percentage"))).alias("stop_loss"),
        ])
        .with_columns([
            ((col("buy_price") - col("stop_loss"))
                + ((col("buy_price") + col("stop_loss")) * commission_percent.clone()))
            .alias("unit_risk"),
            (col("buy_price") * (lit(1.0) + commission_percent.clone())).alias("unit_cost_gross"),
        ])
        .with_columns([
            ((col("capital_total") * col("max_risk_percentage")) / col("unit_risk"))
                .floor()
                .alias("ideal_size_by_risk"),
        ])
        .with_columns([
            (col("ideal_size_by_risk") * col("unit_cost_gross")).alias("ideal_position_cost")
        ])
        // --- ETAP 2: SKALOWANIE DO DOSTĘPNEGO KAPITAŁU (Model Ryzyka Proporcjonalnego) ---
        .with_columns([col("ideal_position_cost")
            .sum()
            .alias("total_ideal_cost_sum")])
        .with_columns([when(col("total_ideal_cost_sum").gt(col("capital_total")))
            .then(col("capital_total") / col("total_ideal_cost_sum"))
            .otherwise(lit(1.0))
            .alias("reduction_factor")])
        .with_columns([(col("ideal_size_by_risk") * col("reduction_factor"))
            .floor()
            .cast(DataType::Int64)
            .alias("position_size")])
        // --- ETAP 3: STATYSTYKI I WYMAGANE KOLUMNY ---
        .with_columns([
            // Koszty zamknięcia (prowizja przy sprzedaży na TP)
            (col("position_size").cast(DataType::Float64)
                * commission_percent.clone()
                * (col("buy_price") + col("target_price")))
            .alias("tp_commission_total"),
            // Wykorzystanie kapitału
            ((col("position_size").cast(DataType::Float64) * col("unit_cost_gross"))
                / col("capital_total")
                * lit(100.0))
            .alias("capital_utilization_pct"),
            // Ryzyko całkowite (strata na kursie + obie prowizje)
            (col("position_size").cast(DataType::Float64) * col("unit_risk"))
                .alias("total_risk_amount"),
        ])
        .with_columns([
            // Zysk netto przed podatkiem
            (col("position_size").cast(DataType::Float64)
                * (col("target_price") - col("buy_price"))
                - col("tp_commission_total"))
            .alias("net_proceeds"),
        ])
        .with_columns([
            // Podatek Belki (naliczany tylko od zysków dodatnich)
            when(col("net_proceeds").gt(0.0))
                .then(col("net_proceeds") * (lit(1.0) - belka_tax))
                .otherwise(col("net_proceeds"))
                .alias("net_profit"),
        ])
        .with_columns([
            // Finalny współczynnik zysku do ryzyka
            (col("net_profit") / col("total_risk_amount")).alias("reward_risk_ratio"),
        ])
        // Zaokrąglanie wyników dla czytelności CSV
        .with_columns([
            col("net_profit").round(ROUND, ROUND_MODE),
            col("reward_risk_ratio").round(2, ROUND_MODE),
            col("capital_utilization_pct").round(ROUND, ROUND_MODE),
            col("stop_loss").round(ROUND, ROUND_MODE),
        ])
        .sort(
            ["reward_risk_ratio"],
            SortMultipleOptions::default().with_order_descending(true),
        )
}
