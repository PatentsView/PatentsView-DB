#!/bin/bash

KEYFILE="$1"

#run disambiguation
ssh -i "$KEYFILE" centos@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; bash run_inventor.sh > disambig.out"