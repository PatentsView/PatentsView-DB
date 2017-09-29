#!/bin/bash

if exist "disambiguator.csv" (
	"cmd /k del disambiguator.csv"
)
echo 'Running consolidation for disambiguator'
python consolidate.py $1
