#!/bin/bash

# Startup file to support ibkhedge0 startup

cd /opt/local/AccountHedge/src


# Uncomment the appropriate execution script based on your configuration.
# If you do not deploy Mongodb you can use the pydblite file based database
# with the Hedge_Batch.bin module. With the Mongodb you can use the Hedge_Active.bin
# module. There will be some manual setup of the Mongodb that will be automated in
# future releases.

if [ $1 = "order" ]
then
for i in {5..5}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $i -f order &)
done
fi

if [ $1 = "account" ]
then
for ii in {6..7}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $ii -f account &)
done
fi

if [ $1 = "option" ]
then
for iii in {8..9}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $iii -f option &)
done
fi

if [ $1 = "all" ]
then
for i in {5..5}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $i -f order &)
done
for ii in {6..7}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $ii -f account &)
done
for iii in {8..9}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $iii -f option &)
done
fi