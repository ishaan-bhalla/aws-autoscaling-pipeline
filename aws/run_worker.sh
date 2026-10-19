#!/usr/bin/env bash

# Modify the variable values below (between quotes) as required
export AWS_REGION="us-east-1"
export WORKER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/12345678/wordfreq-jobs"
export WORKER_RESULT_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/12345678/wordfreq-results"
export WORKER_RESULT_TABLENAME="wordfreq"
export WORKER_MESSAGE_VISIBILITY="200"
# Run worker in the shell
./bin/application