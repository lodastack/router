#!/bin/bash

ps fax | grep router | grep -v grep | awk '{cmd="kill "$1;system(cmd)}'

sleep 2

cd /usr/local/router/ && nohup /usr/local/router/router > /dev/null 2>&1 &