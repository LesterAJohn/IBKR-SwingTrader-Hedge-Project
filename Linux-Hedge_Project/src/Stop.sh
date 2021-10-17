#!/bin/bash

for KILLPID in 'ps ax | grep 'Hedge_Project.bin' | awk '{print $1}''; do 
kill -9 $KILLPID;
done
