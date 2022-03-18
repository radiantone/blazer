#! /usr/bin/env bash
# -*- coding: utf-8 -*-

source ./bin/bash_test_tools

# setup / tear down
function setup
{
  mkdir -p work/parallel
  cd work/parallel
}

function teardown
{
  cd ../..
  rm -rf work
}

# Add test functions here
function test_parallel
{
  mpirun -n 4 ../../venv/bin/python ../../blazer/examples/example10.py 
  # Assert
  
  assert_equal "$?" 0 "exit status"
}

# Run all test functions - optional argument passed to run specific tests only
testrunner
