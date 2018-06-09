#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#to be exected on Proxy server
#./create_instance.sh <collection_name>
solr_dir = "usr/indexer/instance"
echo "solrctl instancedir --generate $solr_dir/$1"
echo "solrctl instancedir --create $1 $solr_dir/$1"
echo "solrctl collection --create $1 -s 1 -r 2"
echo "mv $solrsolr_dir/conf/schema.xml $solrsolr_dir/conf/schema~.xml"
echo "cp /usr/indexer/schema/schema_citifx.xml $solrsolr_dir/conf/schema.xml"
