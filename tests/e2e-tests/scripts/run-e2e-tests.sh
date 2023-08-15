#!/bin/bash

# This script will only run in AWS Codepipeline. It has access to the following environment variables:
# CFN_<OUTPUT-NAME> - Stack output value (replace <OUTPUT-NAME> with the name of the output)
# TEST_REPORT_ABSOLUTE_DIR - Absolute path to where the test report file should be placed
# TEST_REPORT_DIR - Relative path from current directory to where the test report file should be placed
# TEST_ENVIRONMENT - The environment the pipeline is running the tests in

# This file needs to be located at the root when running in the container. The path /test-app is defined
# in the Dockerfile.
cd /test-app || exit 1

export TXMA_QUEUE_URL=$CFN_TXMAQueueURL
export TXMA_BUCKET=$CFN_TXMABucket

npm run e2e-test

TESTS_EXIT_CODE=$?

cp -a test-report $TEST_REPORT_ABSOLUTE_DIR

if [ $TESTS_EXIT_CODE -ne 0 ]; then
  exit 1
fi
