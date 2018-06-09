#!/bin/bash
#Created on: 20180529
#Author: RX
#Usage:
#./delete_instance.sh <solr_hostname> <collection_name> 
echo "http://"$1".nam.nsroot.net:8983/solr/admin/collections?action=DELETE&name=$2"

