# Python scripts for processing USPTO inventor and patent data

The following collection of scripts performs pre- and post-processing on patent
data as part of the patent inventor disambiguation process. Raw patent data is
obtained from [Google Bulk Patent
Download](http://www.google.com/googlebooks/uspto-patents-grants-text.html).

For a high-level overview of the patentprocessor toolchain, please see [our
technical
report](https://github.com/funginstitute/publications/raw/master/patentprocessor/patentprocessor.pdf).

For a description of configuration of the patentprocessor toolchain, please see
[this technical
report](https://github.com/funginstitute/publications/raw/master/weeklyupdate/weeklyupdate.pdf). Note that the
current version of the code has been ported to Windows while the technical report references Mac OS X.

To follow development, subscribe to
[RSS feed](https://github.com/CSSIP-AIR/PatentsProcessorr/commits/master.atom).

## Patentprocessor Overview

There are several steps in the patentprocessor toolchain:

1. Retrieve/locate parsing target
2. Execute parsing phase
3. Run preliminary disambiguations:
    * assignee disambiguation
    * location disambiguation
4. Prepare input for inventor disambiguation
5. Disambiguate inventors (external process)
6. Ingest disambiguated inventors into database

## Installation and Configuration of the Preprocessing Environment

The python-based preprocessor is tested on Windows 2012 R2 Standard; originally it was 
developed and tested on Ubuntu 12.04 and MacOSX 10.6, but has since been ported.

See the [setup instructions in the wiki](https://github.com/CSSIP-AIR/PatentsProcessor/wiki/Setup-Instructions) for details on how to setup an environment for running the patents processor.
