#!/bin/bash

# Startup file to support ibkhedge0 startup

cd /opt/local/AccountHedge/src


# Uncomment the appropriate execution script based on your configuration.
# If you do not deploy Mongodb you can use the pydblite file based database
# with the Hedge_Batch.bin module. With the Mongodb you can use the Hedge_Active.bin
# module. There will be some manual setup of the Mongodb that will be automated in
# future releases.

for i in {3..3}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $i -f order &)
done

for ii in {4..6}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $ii -f account &)
done

for iii in {7..9}
do
	(python3 /opt/local/AccountHedge/src/Hedge_Queue.py -q $iii -f option &)
done
