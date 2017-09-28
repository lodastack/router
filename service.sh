#!/bin/bash

pkill router

sleep 2

cd /usr/local/router/

nohup /usr/local/router/router > /dev/null 2>&1 &
