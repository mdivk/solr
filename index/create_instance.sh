#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#to be exected on Proxy server
#./create_instance.sh <collection_name>
solr_dir = "usr/indexer/instance"
solrctl instancedir --generate $solr_dir/$1
solrctl instancedir --create $1 $solr_dir/$1
solrctl collection --create $1 -s 1 -r 2

