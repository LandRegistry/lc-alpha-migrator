#!/usr/bin/env bash

export SETTINGS="DevelopmentConfig"

source /home/vagrant/venv/migrator/bin/activate

python3 /vagrant/apps/migrator/run.py $1 $2