#!/bin/bash
#Created on: 20180529
#Author: RX
echo "solrctl instancedir --generate $HOME/$1"
echo "solrctl instancedir --create $1 $HOME/$1"
echo "solrctl collection --create $1 -s 1 -r 2"

