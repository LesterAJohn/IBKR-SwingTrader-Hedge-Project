# IBKR-SwingTrader-Hedge-Project
IBKR NinjaTrader Automation Chart Module with Option Writer Linux Hedge Module 

## IBKR Account Requirements
The IBKR account must be a Pro account with a minimum of 35K as this strategy module is configured for daytrading.
The recommnedation is to used the tiered pricing module as it allows for tighter ask/bid spreads. 
The minimum number of recommended position is 25 across several sectors.
It is configured for both pre and post market trading in the US Market (4am EST to 8pm EST) and therefore the Outside of RTH should be set

## IBKR Market Data
Please have market data for all US Market Exchanges registered.

### Reference for IBKR
IBKR Pro Account (https://www.interactivebrokers.com/en/index.php?f=45500)
IBKR Outside of RTH (https://www.interactivebrokers.com/en/index.php?f=47551)
IBKR Market Data (https://www.interactivebrokers.com/en/pricing/research-news-marketdata.php)


# NinjaTrader Strategy Module
The strategy modules is designed to be attached to 1-min chart as it is both a high fequency and swing trader modules that determines which module to use on a case by case basis. It should be in imported into NinjaTrader and then attached to the chart using the 'SL' strategy.

## Back testings
This module does not work with backtesting, but it can be tested using the IBKR Paper Trading account.