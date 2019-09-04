#!/bin/bash

KEYFILE="$1"
FOLDER="$2"


mkdir -p $FOLDER

#inventor
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/exp_out/inventor/disambiguation.postprocessed.tsv "$FOLDER"/inventor_disambiguation.tsv

#assignee
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/exp_out/assignee/disambiguation.tsv "$FOLDER"/assignee_disambiguation.tsv

#location
scp -i "$KEYFILE" disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/exp_out/location/location_post_processed.tsv "$FOLDER"/location_disambiguation.tsv
