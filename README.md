## Trading CLI Tool
This tool automates trading analysis by combining historical order data from mBank with a custom capital risk model to calculate optimal position sizes.

## Execution
To run the analysis, execute the following command in your terminal:
cli.exe ..\data

## Required Data Inputs
The tool requires two CSV files located in the ..\data directory:

1. mBank History Orders ..\data\trade_orders.csv — Contains your historical trade data.

2. Capital Risk Data ..\data\upside.csv — Defines your current strategy and risk parameters.

## Configuration Glossary (upside.csv)
The upside.csv file uses a semicolon-separated format.
Each column is defined as follows:

ticker  - The unique symbol for the security (e.g., ZABKA, AAPL).

buy_price - The intended entry price for the trade.

target_price - Your "Take Profit" goal; the price where you plan to exit with a gain.

stop_loss_percentage - The max % the price can drop from buy_price before closing the trade (expressed as a decimal, e.g., 0.05 for 5%).

capital_total - The total liquid balance available in your trading account.

max_risk_percentage - The % of capital_total you are willing to lose on this specific trade (e.g., 0.02 for 2%).
