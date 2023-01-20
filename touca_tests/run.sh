#!/bin/bash

if [ -z "${TOUCA_API_URL}" ];
then
  echo "TOUCA_API_URL is not set"
  exit -1
fi

if [ -z "${TOUCA_API_KEY}" ];
then
  echo "TOUCA_API_KEY is not set"
  exit -1
fi

if [ -z "${PGACCEL_TOUCA_DATA_DIR}" ];
then
  echo "PGACCEL_TOUCA_DATA_DIR is not set"
  exit -1
fi

TESTS=$(ls queries/ -1p | grep -v / | xargs echo | sed 's/ /,/g')
echo $TESTS

export PATH=$PATH:../build/
run_touca_tests --revision v2.0 --testcase $TESTS
