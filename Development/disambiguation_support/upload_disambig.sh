#!/bin/bash

KEYFILE="$1"
FOLDER="$2"

#upload data
scp -i "$KEYFILE" "$FOLDER"/*.tsv disambiguser@ec2-52-21-62-204.compute-1.amazonaws.com:/data/disambiguation/data