#!/bin/bash

# Integration testing for the consolidate.py script

cd ..

##### Two rows

make spotless > /dev/null
./parse.py -p test/fixtures/xml/ -x ipg120327.two.xml -o .
mkdir -p tmp/integration/ipg120327.two

echo Starting clean...
./run_clean.sh grant

echo Starting consolidate...
python consolidate.py

perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12}\t//' disambiguator.csv
diff test/integration/consolidate/ipg120327.two/disambiguator.csv disambiguator.csv

### 18 rows

make spotless > /dev/null
./parse.py -p test/fixtures/xml/ -x ipg120327.18.xml -o .
mkdir -p tmp/integration/ipg120327.18

echo Starting clean...
./run_clean.sh grant

echo Starting consolidate...
python consolidate.py

perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12}\t//' disambiguator.csv
diff test/integration/consolidate/ipg120327.18/disambiguator.csv disambiguator.csv

## clean up after we're done
rm -rf tmp
make spotless > /dev/null
