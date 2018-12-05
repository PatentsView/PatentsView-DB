#!/bin/bash


scp -i "PatentsView-DB/Development/PV_Apache_Solr.pem centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/data/multi-canopy-output/clean_inventor_results.txt inventor_disambiguation.tsv

scp -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/assignee/disambiguation.post_processed.tsv assignee_disambiguation.tsv

scp -i "PatentsView-DB/Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/location_all.tsv .
