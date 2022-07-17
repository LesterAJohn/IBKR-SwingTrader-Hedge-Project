#!/bin/bash

# Startup file to support ibkhedge0 startup

cd /opt/local/AccountHedge/src
#git pull origin master

# Uncomment the appropriate execution script based on your configuration.
# If you do not deploy Mongodb you can use the pydblite file based database
# with the Hedge_Batch.bin module. With the Mongodb you can use the Hedge_Active.bin
# module. There will be some manual setup of the Mongodb that will be automated in
# future releases.

if [ $1 = "all" ]
then
for i in {1..1}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i -f position &)
done
sleep 10

for ii in {2..2}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $ii -f pnl &)
done
sleep 10

for iii in {3..3}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $iii -f bpm &)
done
sleep 10

for iiii in {4..4}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $iiii -f option &)
done
fi