#!/bin/bash

# Basic Installation - mkdir

sudo mkdir -p /opt/local/AccountHedge/src
sudo mkdir -p /opt/local/AccountHedge/db
sudo mkdir -p /opt/local/AccountHedge/log
sudo mkdir -p /opt/local/env

# Basic Installation - cp

sudo cp -p . /opt/local/AccountHedge/src/.
sudo cp -p /opt/local/env/Env.conf /opt/local/env/Env.bkup
sudo cp -p Env.conf /opt/local/env/.
sudo cp -p hedge.service /usr/lib/systemd/system/.
sudo cp -p hedgeq.service /usr/lib/systemd/system/.

# Basic Installation - deploy Service

sudo systemctl daemon-reload

# Basic - update to crontab for EST timezone

(crontab -l | echo "30 08 * * 1-5 /usr/bin/systemctl start hedge.service && /usr/bin/systemctl start hedgeq.service") | awk '!x[$0]++' | crontab -
(crontab -l | echo "15 16 * * 1-5 /usr/bin/systemctl stop hedge.service && /usr/bin/systemctl stop hedgeq.service") | awk '!x[$0]++' | crontab -

# python / pip installation

sudo apt update
sudo apt upgrade -y
sudo apt install python3 -y
sudo snap install pypy --classic
sudo apt install pip -y
 
sudo ./pipInstallScripts.sh