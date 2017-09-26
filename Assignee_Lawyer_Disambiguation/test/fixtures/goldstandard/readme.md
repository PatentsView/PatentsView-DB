# "Gold Standard" processing for verified patent data

## Benchmark files

* `benchmark.csv` is the csv export from the benchmark.xlsx spreadsheet
  file, converted from Windows `crlf` to unix line convention.
* `benchmark.sh` is a wrapper around some `gawk` which processes the
  csv file to acquire relevant data.

## Gold standard files

* `gs2011.sh` wraps various operations
* `goldstandard.csv` is an input file in a format acceptable to the
  disambiguator.

