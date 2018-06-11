#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#to be executed on Solr server
#./rindex.sh
#conf/conf.indexer defines the location of metadata to be indexed
#Sample of conf.indexer:
#json/apac/apeq/20180401
set -x
POST_JAR_URL=/opt/cloudera/parcels/CDH/jars/post.jar
DOMAIN='.nam.nsroot.net:8983/solr/'
index_command_base="java -Dtype=application/json -Drecursive -Durl="
v1=$(<../conf/conf.solr_server)
echo ${#v1}
v2=$(<../conf/conf.collection)
SOLR_URL=http://$v1$v2'/update/json/docs'
v3=$(<../conf/conf.indexer)
java -Dtype=application/json -Drecursive -Durl="$SOLR_URL" -jar $POST_JAR_URL $v3
$cmd

exit 0
