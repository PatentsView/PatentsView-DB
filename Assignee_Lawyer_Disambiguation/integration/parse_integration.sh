#!/bin/bash

cd ..

echo 'Testing test/fixtures/xml/ipg120327.one.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.one
./parse.py -p test/fixtures/xml/ -x ipg120327.one.xml -o .

for table in application uspatentcitation usapplicationcitation foreigncitation ipcr mainclass otherreference patent rawassignee rawinventor rawlawyer rawlocation subclass uspc usreldoc claim
do
  echo $table 'diffs...'
  sqlite3 -csv grant.db "select * from ${table}"  > tmp/integration/ipg120327.one/${table}.csv
  # remove UUIDs from database dump because these change each time
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipg120327.one/${table}.csv
  diff test/integration/parse/ipg120327.one/${table}.csv tmp/integration/ipg120327.one/${table}.csv
done

echo 'Testing test/fixtures/xml/ipg120327.two.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.two
./parse.py -p test/fixtures/xml/ -x ipg120327.two.xml -o .

for table in application uspatentcitation usapplicationcitation foreigncitation ipcr mainclass otherreference patent rawassignee rawinventor rawlawyer rawlocation subclass uspc usreldoc claim
do
  echo $table 'diffs...'
  sqlite3 -csv grant.db "select * from ${table}"  > tmp/integration/ipg120327.two/${table}.csv
  # remove UUIDs from database dump because these change each time
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipg120327.two/${table}.csv
  diff test/integration/parse/ipg120327.two/${table}.csv tmp/integration/ipg120327.two/${table}.csv
done

echo 'Testing test/fixtures/xml/ipg120327.18.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.18
./parse.py -p test/fixtures/xml/ -x ipg120327.18.xml -o .

for table in application uspatentcitation usapplicationcitation foreigncitation ipcr mainclass otherreference patent rawassignee rawinventor rawlawyer rawlocation subclass uspc usreldoc claim
do
  echo $table 'diffs...'
  sqlite3 -csv grant.db "select * from ${table}"  > tmp/integration/ipg120327.18/${table}.csv
  # remove UUIDs from database dump because these change each time
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipg120327.18/${table}.csv
  diff test/integration/parse/ipg120327.18/${table}.csv tmp/integration/ipg120327.18/${table}.csv
done

echo 'Testing test/fixtures/xml/pa040101.two.xml'
make spotless > /dev/null
mkdir -p tmp/integration/pa040101.two
./parse.py -p test/fixtures/xml/ -x pa040101.two.xml -d application -o .

for table in application mainclass subclass ipcr uspc claim usreldoc rawlocation rawinventor rawassignee
do
  echo $table 'diffs...'
  sqlite3 -csv -header application.db "select * from ${table}"  > tmp/integration/pa040101.two/${table}.csv
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/pa040101.two/${table}.csv
  diff test/integration/parse/pa040101.two/${table}.csv tmp/integration/pa040101.two/${table}.csv
done

echo 'Testing test/fixtures/xml/ipa061228.one.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipa061228.one
./parse.py -p test/fixtures/xml/ -x ipa061228.one.xml -d application -o .

for table in application mainclass subclass ipcr uspc claim usreldoc rawlocation rawinventor rawassignee
do
  echo $table 'diffs...'
  sqlite3 -csv -header application.db "select * from ${table}"  > tmp/integration/ipa061228.one/${table}.csv
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipa061228.one/${table}.csv
  diff test/integration/parse/ipa061228.one/${table}.csv tmp/integration/ipa061228.one/${table}.csv
done

echo 'Testing test/fixtures/xml/ipa130117.one.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipa130117.one
./parse.py -p test/fixtures/xml/ -x ipa130117.one.xml -d application -o .

for table in application mainclass subclass ipcr uspc claim usreldoc rawlocation rawinventor rawassignee
do
  echo $table 'diffs...'
  sqlite3 -csv -header application.db "select * from ${table}"  > tmp/integration/ipa130117.one/${table}.csv
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipa130117.one/${table}.csv
  diff test/integration/parse/ipa130117.one/${table}.csv tmp/integration/ipa130117.one/${table}.csv
done

# clean up after we're done
rm -rf tmp
make spotless > /dev/null
