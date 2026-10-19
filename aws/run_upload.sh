#!/usr/bin/env bash

# Upload a text file to an existing S3 bucket then process messages on the results queue

# Modify the variable values below (between quotes) as required
export AWS_REGION="us-east-1"
export WORKER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/12345678/wordfreq-jobs"
export WORKER_RESULT_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/12345678/wordfreq-results"
export WORKER_RESULT_TABLENAME="wordfreq"

# Check an S3 bucket name is supplied
if [ -z "$1" ]
  then
    echo "No S3 bucket name supplied!"
    echo "Usage: run_upload.sh BUCKET_NAME FILE_TO_UPLOAD"
    exit 1
fi

# Check if an upload file is supplied
if [ -z "$2" ]
  then
    echo "No upload file parameter supplied!"
    echo "Usage: run_upload.sh my_bucket_name upload_file.txt"
    exit 1
fi

# Check the file exists
if [ ! -f "$2" ]
  then
    echo "File to upload not found - check filename/path!"
    exit 1
fi

# Run uploader in the shell with supplied parameters
go run ./cmd/uploads3/main.go "$1" "$2"
