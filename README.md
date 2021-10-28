# IBKR SwingTrader & Option Writer Hedge Modules
IBKR NinjaTrader Trade Automation Module with Option Writer Hedge Module.

This system includes both a NinjaTrader Strategy Module that that operates in several modes, including High Frequency, Swing, and Momentum modes. For trades that meet certain criteria that can be triggered via an Environment Setup file, the Option Writer Hedge Module takes over. It is designed to issue profitable Options that enhance the profits of the NinjaTrader Strategy Module. This is a fully automated Trading System design to operate in multiple modes in support of the different market conditions with no user intervention.


# Required System Components
This is a multi system deployment that requires separate Windows and Linux systems. These systems can be deployed on either Physical or Virtual Machines, but for performance and stablity the minimums must be adhere to.

## Windows 10 System
This system is primarily used for NinjaTrader 8 deployment
- Windows 10 (has not yet been tested with Windows 11)
- 8 Physical or Virtual CPUs
- 24 GB of Memory
- SSD 100GB Harddrive
- 1 Gb Internet Connection
- GPU are not yet enabled as part of the NinjaTrader Platform. Recommendation is to use this component headless.

## Linux System
This system is used for IBKR Trader Workstation, ibcAlpha IBC and Hedge_Project Option Writer Components
- Linux version 8
- 4 Physical or Virtula CPUs
- 8 GB of Memory
- SSD 40GB Harddrive
- 1 Gb Internet Connection


## IBKR Account Requirements (Recommendations)
The IBKR account should be a Pro account with a minimum of 35K as this strategy module has a High-Frequency and Momentum mode that will operate as a Day-Trader. For best results it is recommended that you configure with the the tiered pricing model as it allows for tighter ask/bid spreads and the system is designed to compute these realtime in order to get the best entry and exit pricing. The minimum number of recommended positions is 25 across several sectors. It is capable of supporting more, but that will be dependant on your available computing availablity. The minimum system requirement above are scaled to support 100 positions. The NinjaTrader Strategy Module is configured to operate in pre and post US Market (4am EST to 8pm EST) and therefore the 'Outside of RTH' should be enabled. This account should have the ability to write covered and uncovered options to take full advantage of this system.

## IBKR Market Data
Please have market data for all US Market Exchanges registered. It is used by both modules, but separate market data can be provided only for the NinjaTrader Strategy Module as the Option Writer Hedge Module is dependent on account information from IBKR.

### Reference for IBKR
- IBKR Pro Account (https://www.interactivebrokers.com/en/index.php?f=45500)- 
- IBKR Outside of RTH (https://www.interactivebrokers.com/en/index.php?f=47551)
- IBKR Market Data (https://www.interactivebrokers.com/en/pricing/research-news-marketdata.php)
- IBKR Paper Trading Account (https://www.interactivebrokers.com/en/software/am/am/manageaccount/papertradingaccount.htm)


# NinjaTrader Strategy Module Requirements
This module is designed for Ninjatrader 8 and should be attached to 1-min chart as it operates in serveral modes and determines which mode to use on a case by case basis. The recommendation is to attach the strategy directly to various charts. It is designed to operate in a headless mode as is most strategies.

## NinjaTrader IBKR License
The license should be either leased or purchased as live trading is only available to a fully license product for connection to IBKR. When volumetric trading modes are only supported on purchased licenses.

## NinjaTrader IBKR Connection 
Please see the Ninjatrader IBKR Connection guide and the recommendation is to use the TWS component for connection as oppose to the gateway. Unfortunately IBKR works best with the component as it provides the most stable account and market data API. For the High Fequency mode to function it needs a very stable and consist connection.

## NinjaTrader Import AddOn
The NinjaTrader Module is supplied as a '.zip' file labeled 'TradeAutomation.zip'. Use the import function to pull in the 'SL' Strategy.

## NinjaTrader Create Chart and Add Strategy
While there are several methods to enable a strategy, the recommendation is to create a chart with one or more positions and then attach the strategy to the chart. This method will still create a strategy in the list, but it will also provide a visual representation in the chart and allow you to setup NinjaTrader to operate in a headless mode. See reference below on how to work with strategies in charts.

## NinjaTrader Back Testings
This module uses other market indicators outside of individual position tick date to make buy/sell decisions and therefore NinjaTrading backtesting is not a viable option for testing this module. It is recommended to use IBKR Paper Trading account for testing. The NinjaTrader 8 simulation account is not viable as the Ask/Bid spread is not consistent with the market.

## NinjaTrader Configuration
The module contains a majority of the necessary defaults; howerver, to support the embedded high frequency mode it is important the chart be set to 1-min bars. To support headless operation NinjaTrader can be started via Windows Scheduler and it should be started at least 5 minutes before start of market it is trading in, either 3:50a EST for Pre Market or 9:20a EST for Market trading

NinjaTrader configuration file 'AutoTradeConfig.xml' supports tuning of the application for various modes. Please run the NTConfigSetup.cmd to put the XML Configuration file inplace in 'C:\NinjaTraderConfig' directory.

## NinjaTrader AutoTradeConfig.xml
 - liquidate : This configuration set to 'false' allows the system to open new positions and when set to 'true' it will refrain from opening new positions, but will continue to process existing position as necessary to get them either to profitablity or available for Option Writing Hedges.

### References for NinjaTrader Component
- NinjaTrader Hardware Requirements (https://ninjatrader.com/NinjaTrader-8-InstallationGuide)
- NinjaTrader Software (https://ninjatrader.com/BuyPlatform)
- NinJaTrader Connection Guide (https://ninjatrader.com/ConnectionGuides/Interactive-Brokers-Connection-Guide)
- NinjaTrader Import (https://ninjatrader.com/support/helpGuides/nt8/NT%20HelpGuide%20English.html?import.htm)
- NinjaTrader Chart Creation (https://ninjatrader.com/support/helpGuides/nt8/NT%20HelpGuide%20English.html?creating_a_chart.htm)
- NinjaTrader Chart Strategy (https://ninjatrader.com/support/helpGuides/nt8/NT%20HelpGuide%20English.html?strategy.htm)


# IBKR Trader Workstation Component
In order to support the NinjaTrader Strategy Module and the Option Writer components you need to run the Trader Workstation component. It is recommended you running this component on the Linux system and configure it to be deploy headless using the IBC component.

## Configuration
Once TWS is deploy the TWS API must be enabled and configured to allow local connections. Use the reference below to use the complete the configuration. 

### Reference for Trader Workstation
- IBKR Trader Workstation version (https://www.interactivebrokers.com/en/index.php?f=16045)
- IBKR Trader Workstation installation (https://www.interactivebrokers.com/en/index.php?f=17863)
- IBKR Trader Workstation API Configuration (https://interactivebrokers.github.io/tws-api/initial_setup.html)
- IBC (https://github.com/IbcAlpha/IBC)


# IBKR Option Writer Hedge Component
The Option Writer component performs best on the Linux systems that is shared with TWS/IBC components. It is designed to write options against long/short position of 100 or more shares. It works in conjunction with the NinjaTrader Strategy Module to properly size and managing positions that are currently hedged.

## Installation
The deployment script "hedgeInstallation.sh" should be executed with root permission and it will create directories and deploy the necessary components as well as the cron start and stop jobs in the EST timezone.

The environment configuration file is located in '/opt/local/env/Env.conf' and prior to start the Account # should be updated and any other defaults should be changed.  

To start the system manually you can use 'systemctl start/stop hedge' service that is deployed by 'HedgeInstallation.sh' script. It is designed to be deployed to the '/opt/local/' structure.


# Latest Improvements
10.28.2021
### NinjaTrader Module
- XML configuration added for Liquidation Status
- XML configuration added for Option Ready Trigger amount
- BuySell Pressure entry order function added
- Enhanced Hold Exit trade function to improve position profits

### Option Writer Module
- Added configuration to tune connection disconnect cycle time
- Added file lock management to improve Database read/write speed
- Better handling of changed option status from ‘Hedge’ to ‘Naked’
- Continuous Profit and Loss calculations
- Continuous Ask/Bid calculations
