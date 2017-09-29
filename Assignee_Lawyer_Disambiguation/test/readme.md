# Test suite for patent preprocessing

#### tl;dr

* `./pattesttest.sh`
* `./parse_integration.sh`
* `./clean_integration.sh`
* `./consolidation_integration.sh`

----


We're intending on conforming to PEP guidelines,
please note where implementation is not meeting
a relevant PEP.

Currently (January 30, 2013), we're running unit tests and integration
tests. We do not have full coverage for unit tests. Unit tests are being
constructed as part of the refactoring process, and all new code should
be covered in unit tests.

Integration tests will run end-to-end on the parsing, cleaning and consolidation
phases, but the current data sets used in the integration tests are
incomplete. Further, the location handling does not work correctly, so
the integration test covering geocoding is broken by designed.

## Running unit tests

Unit tests are constructed for two specific reasons:

1. Prevent regression as code base is refactored, and
2. Ensure extensions to the current code work correctly.

A general explanation of either refactoring or unit testing new code is
beyond the scope of this readme. File an enhancement request with
specific questions you would like to have answered in this readme.

The unit tests are invoked automatically in the `./patenttest.sh`
script.


### PATENTROOT

Not having `PATENTROOT` set will produce this warning notice:

```sh
Processing test_parse_config.py file..
Cannot find PATENTROOT environment variable. Setting PATENTROOT to the
patentprocessor directory for the scope of this test. Use `export
PATENTROOT=/path/to/directory` to change
```

This is easy to silence: `$ export PATENTROOT=.`

You may want to export `PATENTROOT` in your shell initialization script
for convenience.


## Running integration tests

Integration testing for the patent preprocessor simulates running both
preprocessor components and the entire preprocessor on a limited set of
patent data. The goal is ensuring that for a given input, the output
doesn't change from run to run as the code continues development.

The integration tests require two types of databases:

1. A set of sqlite databases located in the test directory as a result
   of a succesful parse, and
2. Databases `loctbl` and `NBER_asg` linked from elsewhere like so:
    * `ln -s /data/patentdata/NBER/NBER_asg .`
    * `ln -s /data/patentdata/location/loctbl.sqlite3 loctbl`
   (Your links may be different.)

The databases mentioned in item 1 are constructed during the
preprocessing, and require no initial setup.

The databases mentioned in item 2 are used in the cleaning phase of the
preprocessor. 

Fung Institute developers have access to both `loctbl` and `NBER_asg` on
the server. These are read-only on the server, and should be copied into
user's home areas with the soft adjusted appropriately.

External developers and other interested parties can download:

* [loctbl](https://s3-us-west-1.amazonaws.com/fidownloads/loctbl.sqlite3)
* [NBER_asg](https://s3-us-west-1.amazonaws.com/fidownloads/NBER_asg)

Note: the integration tests pass, that is, run correctly, for data we
know is not 100% correct. However, these tests allow evolving the code
to correctness incrementally.



#### Test speed

The integration tests require correctly indexed tables to operate
efficiently. The run time difference is roughly 5 minutes for each test
over the geocoding with unindexed tables, versus about 6 seconds for
correctly indexed tables.
