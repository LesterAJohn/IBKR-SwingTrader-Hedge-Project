Echo "Creates NinjaTraderConfig Directory and moves in Default xml configuration file"

mkdir "C:\\NinjaTraderConfig"
copy /V /Y AutoTradeConfig.xml /B  "C:\\NinjaTraderConfig\\AutoTradeConfig.xml" /B
