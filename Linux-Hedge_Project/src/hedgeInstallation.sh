#!/bin/bash

# Basic Installation - mkdir

sudo mkdir -p /opt/local/AccountHedge/src
sudo mkdir -p /opt/local/AccountHedge/db
sudo mkdir -p /opt/local/AccountHedge/log
sudo mkdir -p /opt/local/env

# Basic Installation - cp

sudo cp -p . /opt/local/AccountHedge/src/.
sudo cp -p Env.conf /opt/local/env/.
sudo cp -p hedge.service /usr/lib/systemd/system/.

# Basic Installation - deploy Service

sudo systemctl daemon-reload

# Basic - update to crontab for EST timezone

(crontab -l | echo "35 09 * * 1-5 /usr/bin/systemctl start hedge.service") | awk '!x[$0]++' | crontab -
(crontab -l | echo "05 16 * * 1-5 /usr/bin/systemctl stop hedge.service") | awk '!x[$0]++' | crontab -

# python / pip installation

sudo apt install python3 -y 
sudo apt install pip -y
 
sudo ./pipInstallScripts.sh