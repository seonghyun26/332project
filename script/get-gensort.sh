#!/bin/bash

wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xzf gensort-linux-1.5.tar.gz && rm -f gensort-linux-1.5.tar.gz
rm -rf 32 gensort
mv 64 gensort

# # Git clone and build gensort
# git clone https://github.com/petergeoghegan/gensort.git ./gensort
# cd ./gensort
# make
