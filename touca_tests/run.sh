#!/bin/bash

if [ -z "${PGACCEL_TOUCA_DATA_DIR}" ];
then
  echo "PGACCEL_TOUCA_DATA_DIR is not set"
  exit -1
fi

TESTS=$(ls queries/ -1p | grep -v / | xargs echo | sed 's/ /,/g')
echo $TESTS

export PATH=$PATH:../build/
run_touca_tests --testcase $TESTS
