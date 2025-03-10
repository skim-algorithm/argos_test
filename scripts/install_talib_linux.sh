#!/usr/bin/env bash
apt install build-essential wget -y
wget https://sourceforge.net/projects/ta-lib/files/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz
tar -xvf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr
make
make install
rm ../ta-lib-0.4.0-src.tar.gz