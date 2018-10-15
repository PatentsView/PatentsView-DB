#!/bin/bash

scp -i "Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/assignee/disambiguation.post_processed.tsv assignee_disambiguation.tsv

scp -i "Development/PV_Apache_Solr.pem" centos@ec2-52-21-62-204.compute-1.amazonaws.com:/data/inventor-disambiguation-internal/exp_out/location_all.tsv .
