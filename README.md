# IBKR SwingTrader & Hedge Project
IBKR NinjaTrader Trade Automation Chart Module with Option Writer Linux Hedge Module.

This system includes both a NinjaTrader Chart Module Strategy that support both high frequency and swing trade modes. When a trade does not go as planned the Option Writer component takes over to write options against stock position that are going negative. It is designed to issues options within 2 months and watches those options to expiration. It will determine if the option should be closed via purchase, expiration or assignment. If the option is assigned the NinjaTrader module will take over management and the Option Writer will write an option against the positions if appropriate.


# Required System Components
This is a multi system deployment and it requires an individual Windows and Linux system. They can be Physical or Virtual Machines and it has been tested on VMware Workstation deployed on Linux or VMware ESXi. If a cloud environment is used it is important that the IP addresses be static.

## Windows 10 System
This system is primarily used for NinjaTrader 8 deployment
- Windows 10 (has not yet been tested with Windows 11)
- 8 Physical or Virtual CPUs
- 16 GB of Memory
- SSD 100GB Harddrive
- 1 Gb Internet Connection
- GPU are not yet enabled as part of the NinjaTrader Platform. Recommendation is to use this component headless.

## Linux System
This system is used for IBKR Trader Workstation and for the Hedge_Project.bin Option Writer Component
- Linux version 8
- 4 Physical or Virtula CPUs
- 8 GB of Memory
- SSD 40GB Harddrive
- 1 Gb Internet Connection


## IBKR Account Requirements
The IBKR account must be a Pro account with a minimum of 35K as this strategy module is configured for Day-Trading. The recommendation is to use the tiered pricing model as it allows for tighter ask/bid spreads. The minimum number of recommended positions is 25 across several sectors. It is configured for both pre and post market trading in the US Market (4am EST to 8pm EST) and therefore the Outside of RTH should be enabled.

## IBKR Market Data
Please have market data for all US Market Exchanges registered.

### Reference for IBKR
- IBKR Pro Account (https://www.interactivebrokers.com/en/index.php?f=45500)- 
- IBKR Outside of RTH (https://www.interactivebrokers.com/en/index.php?f=47551)
- IBKR Market Data (https://www.interactivebrokers.com/en/pricing/research-news-marketdata.php)
- IBKR Paper Trading Account (https://www.interactivebrokers.com/en/software/am/am/manageaccount/papertradingaccount.htm)


# NinjaTrader Strategy Module Requirement
This module is designed for Ninjatrader 8 and should be attached to 1-min chart as it is both a high fequency and swing trader modules that determines which module to use on a case by case basis. It should be in imported into NinjaTrader and then attached to the chart using the 'SL' strategy.

## NinjaTrader IBKR License
The license should be either leased or purchased as live trading is only available to a fully license product for connection to IBKR.

## NinjaTrader IBKR Connection 
Please see the Ninjatrader IBKR Connection guide and the recommendation is to use the TWS component for connection as oppose to the gateway. Unfortunately IBKR works best with the component as it provides the most stable account and market data API. For the High Fequency mode to function it needs a very stable and consist connection.

## Back Testings
This module uses other market indicators outside of individual position tick date to make buy/sell decisions and therefore NinjaTrading backtesting is not a viable option for testing this module. It is recommended to use IBKR Paper Trading account for testing. The NinjaTrader 8 simulation account is not viable as the Ask/Bid spread is not consistent with the market.

## Configuration
The module contains a majority of the necessary defaults; howerver, to support the embedded high frequency mode it is important the chart be set to 1-min bars. In order to run NinjaTrader headless you can use Windows Scheduler. 

### Reference for NinjaTrader
- NinjaTrader Hardware Requirements (https://ninjatrader.com/NinjaTrader-8-InstallationGuide)
- NinjaTrader Software (https://ninjatrader.com/BuyPlatform)
- NinJaTrader Connection Guide (https://ninjatrader.com/ConnectionGuides/Interactive-Brokers-Connection-Guide)
