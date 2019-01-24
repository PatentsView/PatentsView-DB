#!/bin/bash

echo "Just running assignee"

echo 'Running assignee disambiguation'
python lib/assignee_disambiguation.py

REM TODO: fixup lawyer disambiguation
REM echo 'Running lawyer disambiguation'
REM python lib/lawyer_disambiguation.py grant
