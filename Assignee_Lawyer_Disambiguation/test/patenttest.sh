#!/bin/bash

if [ -f /tmp/res ] ;
  then rm /tmp/res ;
fi

for f in test_*.py
do
  printf "\e[0m"
  echo "Processing $f file.."
  python $f &> /tmp/res
  if [[ $? == 0 ]] ;
    then printf "\e[32m" ;
    else printf "\e[31m" ;
  fi
  cat /tmp/res
done

if [ -f /tmp/res ] ;
  then rm /tmp/res ;
fi
printf "\e[0m"

rm -rf *.sqlite3 *.pyc
rm -rf ../*.sqlite3 *.pyc
