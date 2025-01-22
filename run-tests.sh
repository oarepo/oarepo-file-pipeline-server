#!/bin/bash

PYTHON="${PYTHON:-python3.12}"

set -e

OAREPO_VERSION="${OAREPO_VERSION:-12}"

export VENV=".venv"

setup_test_venv(){
(
  echo "Setting up the testing venv"

  if test -d $VENV ; then
    rm -rf $VENV
  fi

  $PYTHON -m venv $VENV
  . $VENV/bin/activate
  pip install -U setuptools pip wheel

  pip install -e ".[tests]"
  pip install aiohttp
  pip install stream-zip
  pip install pillow
  pip install pytest-random-order
  echo "Testing venv is set up"
)
}

run_tests(){
  . $VENV/bin/activate
  echo "Running tests..."
  pytest tests --random-order -v
}

if [ "$0" = "$BASH_SOURCE" ]; then
    setup_test_venv
    run_tests
else
    echo "This script includes the following functions:"
    echo "5. setup_test_venv: Sets up a virtual environment for testing."
    echo "6. run_tests: Runs pytest."
    echo ""
    echo "To execute the script, run it directly: ./run-tests.sh"
fi