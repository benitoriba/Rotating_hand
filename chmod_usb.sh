#!/bin/bash

sudo chmod 666 /dev/ttyUSB0
sudo chmod 666 /dev/ttyUSB1
sudo chmod 666 /dev/ttyUSB2


echo "Permissions updated for ttyUSB0, ttyUSB1, ttyUSB2"
ls -l /dev/ttyUSB0 /dev/ttyUSB1 /dev/ttyUSB2