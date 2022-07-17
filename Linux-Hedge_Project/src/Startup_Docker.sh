#!/bin/bash

# Startup file to support ibkhedge0 startup

cd /opt/local/AccountHedge/src

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

for i1 in {2..2}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i1 -f pnl &)
done
sleep 10

for i2 in {3..3}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i2 -f bpm &)
done
sleep 10

for i3 in {4..4}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i3 -f option &)
done

for i4 in {5..5}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i4 -f batch &)
done

for i5 in {6..6}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i5 -f contract &)
done

for i6 in {7..7}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i6 -f order &)
done

for i7 in {8..9}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i7 -f accountHis &)
	sleep 10
done

for i8 in {10..12}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Active.py -c $i8 -f optionHis &)
	sleep 10
done

fi