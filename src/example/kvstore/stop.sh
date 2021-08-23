#!/bin/bash

set -xe

for pid in `screen -list |grep node-|awk -F '.' '{print $1}'`;do
	echo $pid
	kill -9 ${pid}
done
screen -wipe
sync
screen -list
