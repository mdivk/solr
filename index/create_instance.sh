#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#to be exected on Proxy server
#./create_instance.sh <collection_name>
solrctl instancedir --generate $HOME/$1
solrctl instancedir --create $1 $HOME/$1
solrctl collection --create $1 -s 1 -r 2

