#!/bin/bash

# Startup file to support ibkhedge0 startup

cd /opt/local/AccountHedge/src

# Uncomment the appropriate execution script based on your configuration.
# If you do not deploy Mongodb you can use the pydblite file based database
# with the Hedge_Batch.bin module. With the Mongodb you can use the Hedge_Active.bin
# module. There will be some manual setup of the Mongodb that will be automated in
# future releases.

if [ $1 = "batch" ]
then
for i in {3..3}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i -f batch &)
done
fi

if [ $1 = "contract" ]
then
for ii in {10..10}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $ii -f contract &)
done
fi

if [ $1 = "all" ]
then
for i in {3..3}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i -f batch &)
done
for ii in {10..10}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $ii -f contract &)
done
fi