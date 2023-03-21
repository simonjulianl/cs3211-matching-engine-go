#!/bin/bash

make clean
make
COUNTER=1
for i in tests/*; do
  echo -e "\n=========== TEST CASE ${COUNTER}: ${i} ==========="
  ./grader ./engine < ${i}
  let COUNTER+=1
done