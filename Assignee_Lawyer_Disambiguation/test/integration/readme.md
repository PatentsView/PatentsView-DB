# Integration testing for patent processing

This integration directory stores known, good outputs
from scripts running end to end.

### General procedure for generating a test

1. Run `preprocess.sh` on a limited, known input.
2. Export results from 1 or more inputs from 1 into csv.
3. Commit appropriate known, correct results into repo.
4. Write a wrapper script for running the integration test
and checking output with (say) diff, automatically. This script
could be written in sh, ruby or python, but preferably not sh.

