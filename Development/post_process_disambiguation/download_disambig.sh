#!/bin/bash

KEYFILE="$1"
FOLDER="$2"


mkdir -p $FOLDER

#inventor
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/data/multi-canopy-output/clean_inventor_results.txt "$FOLDER"/inventor_disambiguation.tsv

#assignee
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/exp_out/assignee/disambiguation.post_processed.tsv "$FOLDER"/assignee_disambiguation.tsv

#location
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/exp_out/inventor/location_all.tsv "$FOLDER"/location_disambiguation.tsv
