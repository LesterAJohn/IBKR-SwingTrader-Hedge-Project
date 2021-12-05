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

# Basic Installation - deploy Service

sudo systemctl daemon-reload

# Basic - update to crontab for EST timezone

(crontab -l | echo "30 09 * * 1-5 /usr/bin/systemctl start hedge.service") | awk '!x[$0]++' | crontab -
(crontab -l | echo "00 16 * * 1-5 /usr/bin/systemctl stop hedge.service") | awk '!x[$0]++' | crontab -

# python / pip / mongodb installation

wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -
echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list

sudo apt update
sudo apt upgrade -y
sudo apt install python3 -y 
sudo apt install pip -y
sudo apt install mongodb-org -y
 
sudo ./pipInstallScripts.sh