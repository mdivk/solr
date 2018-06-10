#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#to be executed on Solr server
#./sindex.sh 
#conf/conf.indexer defines the location of metadata to be indexed
#Sample of conf.indexer:
#json/apac/apeq/20180401

POST_JAR_URL='opt/cloudera/parcels/CDH/jars/post.jar'
DOMAIN='.nam.nsroot.net:8983/solr/'
index_command_base='java -Dtype=application/json -Drecursive -Durl='
v=$(<conf.indexer)
echo $index_command_base$SOLR_URL' -jar '$POST_JAR_URL $v
