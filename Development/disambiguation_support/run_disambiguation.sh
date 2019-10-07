#!/bin/bash

KEYFILE="$1"

#run disambiguation
ssh -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/disambiguation; bash run_all.sh > run_all.out 2> run_all.err < /dev/null"