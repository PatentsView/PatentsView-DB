#!/bin/bash

KEYFILE="$1"

#run disambiguation
ssh -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com "cd /data/inventor-disambiguation-internal; sudo bash run_all.sh > disambig.out"