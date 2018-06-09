#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#./delete_instance.sh <solr_hostname> <collection_name> 
echo "curl http://"$1.nam.nsroot.net:8983/solr/admin/collections?action=DELETE&name=$2
echo "solrctl instancedir --delete $2"
echo "rm -r /usr/indexer/instance/$2"

