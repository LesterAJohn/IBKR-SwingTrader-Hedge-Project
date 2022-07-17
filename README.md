# IBKR SwingTrader & Option Writer Hedge Modules
IBKR NinjaTrader Trade Automation Module with Option Writer Hedge Module.

This system includes a NinjaTrader Strategy Module that operates in several modes, including High Frequency, Swing, and Momentum modes. This is an automated Stock Trading Module that in the future I will be porting from NinjaTrader Platform C++ to something else based on where I can get the libraries I need for the algorithmic compute functions leveraging a GPU.

In any trading application you need to manage entry points and losses. For the NinjaTrader Module that was ported from another platform I initially I managed losses with stops that supported all or nothing exits. I have moved away from all or nothing position exits as I found them unnecessarily wasteful as it didn't take in to account recovery or other trading opportunities such as option hedges as way to profit from losing position.

The Option Writer Module is the second component of this system that hedges loss trades by writing options against them as a way to continue to create profits while you wait for stock position to recover or to slowly be removed from the systems by the NinjaTrader Strategy Module at the appropriate times.

Due to upgrades in Python libraries the compiled version of the Hedge_Active and Hedge_Queue modules is no longer possible at this time, therefore I will be releasing the raw python code going forward. This is not optimized code and as this is a side project it is not as clean as I want it to be as it is a weekend project. You will also see stub code for future enhancement that I am think about that I have not yet fully formed. As an example there is a Crypto Module that is being built out and may endup in another module, the purpose of that module will be to leverage cash in the account to increase the overall portfolio profit(stay tuned). 

The system count for this environment includes a dedicated Linux based MarketData, Database and Application Servers in addition to the Windows Host for the NinjaTrader Platform.


# Required System Components
This is a multi-system deployment that requires separate Windows and Linux systems. These systems can be deployed on either Physical or Virtual Machines. 

## Windows Pro 10/11 Application System
This system is primarily used for NinjaTrader 8 deployment
- Windows Pro 10 or 11
- 32 Physical or Virtual CPUs
- 32 GB of Memory
- SSD 100GB Storage
- 1 Gb Internet Connection
- GPU are not used for much of the compute in the NinjaTrader Platform. Recommendation is to run this headless.

Note: If you are running a VM instance using Windows 10 pro, you are limited to 2 socks and if you have more than 8 cores the system splits into 2 compute and memory nodes. 
Example:
2 sockets / 4 Cores each with 32gb Memory give you full access to the 8 cores and 32gb of Memory
2 sockets / 6 Cores each with 32gb Memory give you 2 nodes with 6 cores and 16gb Memory. Applications such as the NinjaTrader platform will run in one of the Node spaces; thereby reducing your compute for that individual application.

If you cross over 8 cores in VM you will need to increase your virtual cpus and memory to compensate for the split. The recommendation at this time is to add another Windows Application System and NinjaTrader Licenses to increase compute for the windows module.

## Linux MarketData System
This system is used for IBKR Trader Workstation, ibcAlpha IBC Components
- Linux version 8
- 2 Physical or Virtual CPUs
- 4 GB of Memory
- SSD 40GB Storage
- 1 Gb Internet Connection

## Linux Application System
This system is used for the Hedge_Project Option Writer Component
- Linux version 8
- 8 Physical or Virtual CPUs
- 8 GB of Memory
- SSD 40GB Storage
- 1 Gb Internet Connection

## Linux Database System
This system is used for Mongodb Community Edition
- Linux version 8
- 4 Physical or Virtual CPUs
- 8 GB of Memory
- SSD 60GB Storage
- 1 Gb Internet Connection

## IBKR Account Requirements (Recommendations)
The IBKR account should be a Pro account with a minimum of 35K as this strategy module has a High-Frequency and Momentum mode that will operate as a Day-Trader. For best results it is recommended that you configure with the the tiered pricing model as it allows for tighter ask/bid spreads and the system is designed to compute these realtime in order to get the best entry and exit pricing. The minimum number of recommended positions is 25 across several sectors. It is capable of supporting more, but that will be depend on your available compute. The minimum system requirements above are scaled to support 300 positions. The NinjaTrader Strategy Module is configured to operate in pre and post US Market (4am EST to 8pm EST) and therefore the 'Outside of RTH' should be enabled. This account should have the ability to write covered and uncovered options to take full advantage of this system.

## IBKR Market Data
Please have market data for all US Market Exchanges registered. It is used by both modules, but separate market data can be provided only for the NinjaTrader Strategy Module as the Option Writer Hedge Module is dependent on account information from IBKR.

### Reference for IBKR
- IBKR Pro Account (https://www.interactivebrokers.com/en/index.php?f=45500)- 
- IBKR Outside of RTH (https://www.interactivebrokers.com/en/index.php?f=47551)
- IBKR Market Data (https://www.interactivebrokers.com/en/pricing/research-news-marketdata.php)
- IBKR Paper Trading Account (https://www.interactivebrokers.com/en/software/am/am/manageaccount/papertradingaccount.htm)


## Mongodb Community Edition (Recommendations)
The Database was selected due to the capabilities of the library to manage the queue.

### Reference for Mongodb Community Edition
- Mongodb Community Edition (https://docs.mongodb.com/v5.0/tutorial/install-mongodb-on-debian/)
- Mongodb Docker Image (https://hub.docker.com/_/mongo)


# NinjaTrader Strategy Module Requirements
This module is designed for Ninjatrader 8 and should be attached to 1-min chart as it operates in several modes and determines which mode to use on a case by case basis. The recommendation is to attach the strategy directly to various charts. It is designed to operate in a headless mode as is most strategies.

## NinjaTrader IBKR License
The license should be either leased or purchased as live trading is only available to a fully license product for connection to IBKR. When enabling volumetric trading modes they are only supported on purchased licenses.

## NinjaTrader IBKR Connection 
Please see the Ninjatrader IBKR Connection guide and the recommendation is to use the TWS component for connection as oppose to the gateway. Unfortunately IBKR works best with the component as it provides the most stable account and market data API. For the High Frequency mode to function it needs a very stable and consist connection.

## NinjaTrader Import AddOn
The NinjaTrader Module is supplied as a '.zip' file labeled 'TradeAutomation.zip'. Use the import function to pull in the 'SL' Strategy.

## NinjaTrader Create Chart and Add Strategy
While there are several methods to enable a strategy, the recommendation is to create a chart with one or more positions and then attach the strategy to the chart. This method will still create a strategy in the list, but it will also provide a visual representation in the chart and allow you to setup NinjaTrader to operate in a headless mode. See reference below on how to work with strategies in charts.

## NinjaTrader Back Testings
This module uses other market indicators outside of individual position tick date to make buy/sell decisions and therefore NinjaTrading backtesting is not a viable option for testing this module. It is recommended to use IBKR Paper Trading account for testing. The NinjaTrader 8 simulation account is not viable as the Ask/Bid spread is not consistent with the market.

## NinjaTrader Configuration
The module contains a majority of the necessary defaults; however, to support the embedded high frequency mode it is important the chart be set to 1-min bars. To support headless operation NinjaTrader can be started via Windows Scheduler and it should be started at least 10 minutes before start of market it is trading in, either 3:50a EST for Pre Market or 9:20a EST for Market trading. The strategy preloads 256 1-min and 256 5-min bars to enabled the strategy for each position and that does take time.

NinjaTrader configuration file 'AutoTradeConfig.xml' supports tuning of the application for various modes. Please run the NTConfigSetup.cmd to put the XML Configuration file inplace in 'C:\NinjaTraderConfig' directory.

## NinjaTrader AutoTradeConfig.xml
 - liquidate : This configuration set to 'false' allows the system to open new positions and when set to 'true' it will refrain from opening new positions, but will continue to process existing position as necessary to get them either to profitability or available for Option Writing Hedges.

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
The Option Writer component performs best on separate Linux systems. On a shared system he thread count needed to process realtime data in the Option Thread Module it will take resources form the TWS components and reduce the performance of the overall system; therefore a separate system is recommended. It is designed to write/hedge options against long/short position of 100 or more shares.

## Installation (quick)
- The deployment script "hedgeInstallation.sh" should be executed with root permission and it will create directories and deploy the necessary components as well as the cron start and stop jobs in the EST timezone.
- The environment configuration file is located in '/opt/local/env/Env.conf' and prior to start the Account # should be updated and any other defaults should be changed.  
- The Env.conf file allow you to select the name of MongoDB database. The Database name you chose and the following collections: 'Account' / 'Options' / 'Orders' need to be created manually.
- To start the system manually you can use 'systemctl start/stop hedge' service that is deployed by 'HedgeInstallation.sh' script. It is designed to be deployed to the '/opt/local/' structure.


# Latest Improvements
#### NinjaTrader Module - Date 10.27.2021
- XML configuration added for Liquidation Status
- XML configuration added for Option Ready Trigger amount
- BuySell Pressure entry order function added
- Enhanced Hold Exit trade function to improve position profits

#### Option Writer Module - Date 10.27.2021
- Added configuration to tune connection disconnect cycle time
- Added file lock management to improve Database read/write speed
- Better handling of changed option status from ‘Hedge’ to ‘Naked’
- Continuous Profit and Loss calculations
- Continuous Ask/Bid calculations

#### NinjaTrader Module - Date 10.28.2021
- Changed the OptionTrigger to OptionTriggerPer. The Option Trigger is now a floating value based on the overall value of account

#### Option Writer Module - Date 10.30.2021
- Added configuration item "PNLTRIGGERPER" that is used to set percentage of Net Liquidity as target start for position ready for Options. Default is set to 0.1%.
- System will adjust entry target based on overall account value
- Added option profit targets reporting of 90% for Hedge Positions and 50% for Naked Options to statistics download "Hedge_Stat.bin" script

#### Option Writer Module - Date 10.31.2021
- Multi-directional hedge functionality added to support positions that change from long to short during option period

#### Option Writer Module - Date 11.04.2021
- Ask/Bid calculation based on avgCost and unRealizedPnL for options that provide no recent data Ask/Bid data

#### Option Writer Module - Date 11.14.2021
- Improved Option DB data timeout settings to avoid mass timeouts
- Improved Hedge_Stat module order predication timing calculations
- Added additional function threading

#### Option Writer Module - Date 12.05.2021
- Finalized code for the Hedge_Batch.bin Module
- Added a Mongo DB backend to support the Hedge_Active.bin Module in order to handle multi-threading of read and writes functions
- Added Hedge_Active.bin (Alpha Code) module that leverages activity triggers and multiple threads 

#### Option Writer Module - Date 12.12.2021
- Beta Code for Hedge_Batch.bin and Hedge_Active Modules.
- Hedge_Batch.bin uses a file based daily database.
- Hedge_Active.bin (Preferred) uses the Mongodb database for storage.
- Default cycletime adjusted to 5 minutes / 300 seconds in Env.conf file

#### Option Writer Module - Date 12.15.2021
- Streams Account, Position and Profit and Loss Information
- Added Historical Queue Collection to Mongodb
- Added Configuration Object to determine batch size of Ask / Bid Information

#### Option Writer Module - Date 12.16.2021
- Increased threading in order to parallel reads from the IBKR Queue

#### Option Writer Module - Date 12.19.2021
- Active Module includes better IBKR Queue management
- Improved Thread Management
- Batch Module now uses a single connect and no longer disconnects each cycle
- Update ENV.CONF file to support IBKR Historical Data Request Limits

#### Option Writer Module - Date 02.11.2022 [ Significant Changes ]
A lot has changed as this is now supporting streaming data across several python -> TWS connections. This is a quick summary of the changes, but now that the raw code is included realize that I have yet to optimize as well as take out stub code for future enhancement that I am developing such as the crypto module and startegy that is coming.

Changes:
- Hedge Active Stream Process designed to handled streaming data from IBKR reader queue such as Position Updates / PnL Updates / Account Profit and Exit Calculations
- Hedge Queue Batch Process that separates in to 3 functions
	Account - Handles Orders and Account Position Updates
	Historical Account - Handles request for Ask/Bid data from IBKR API for Active Account Positions that are ready for Option Hedge Orders
	Historical Option - Handles request for Ask/Bid data from IBKR API for Option Contracts
- New Startup Services and Scripts
	hedge.service and hedgeq.service system scripts configure the required settings for each component in conjunction with the Startup.sh and Startup_Q.sh
	Start_Q.sh launch multiple versions for Hedge_Queue.py with different configuration parameters based on its use.
- System configuration Scripts for Application Server
- System configuration Scripts for Mongodb Database Server

What started out as a weekend project is now much larger, so I owe a much more detailed deployment and tuning manual for this platform.

#### Option Writer Module - Date 02.20.2022
- Active Module is now split between Streaming and Batch Functions
- Active Module Batch functions leverage multi-threading for faster processing
- Improved use of Database connections

#### Option Writer Module - Date 02.22.2022
- Active Module performance and data update improvements to create a faster system.
- Database connection issues have been resolved and system now runs stably on a single linux application server with 6 VCPUs

#### Option Writer Module - Date 02.23.2022
- Added variable "RELOADPOSITIONS" to configuration file in order to allow for customization of this timing

#### Option Writer Module - Date 03.14.2022
- Added Buy Power Management recovery functions. This feature currently has a variable limit at 1% floor that if breached the system will start looking for position to close.
- Separated Position Updates into it own process.
- Increased threading in Historical Queue functions. In order to support the additional position associated with BPM, performance in historical data had to be increased and therefore the move to threads for the historical queue management.
- Modified startup scripts to allows additional separation of function across additional Linux Application Servers

#### Option Writer Module - Date 03.15.2022
- Completed ProcessQueue items prior to issuing orders

#### Option Writer Module - Date 03.31.2022
Added the following separations to improve performance:
- position - batch download of account positions
- pnl - subscription to account position for possible hedge
- bpm - subscription to account position for possible buy power management transactions
- batch - computation functions for decisions on hedge and option and buy power management transactions
- contract - option chain download function
- order - order execution
- account - ask / bid request for account positions
- option - ask / bid request for selected option chains

#### Option Writer Module - Date 07.16.2022
Enhances functionality with intro of TimeSeries Database
