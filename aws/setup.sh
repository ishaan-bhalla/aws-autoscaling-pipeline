#!/usr/bin/env bash

export AWS_REGION="us-east-1"

# Set up the GOLANG environment
echo
echo "WordFreq Setup:"
echo "Installing the GO language environment..."
cd /tmp
wget https://golang.org/dl/go1.5.1.linux-amd64.tar.gz
tar -xvf go1.5.1.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo mv go /usr/local/
cd -

# Copy over and run the profile for environment variables
cp -f go-profile ~/.profile
if [ -f ~/.bashrc ]; then cat go-bashrc >> ~/.bashrc; fi
source ~/.profile

# Install the GOLANG AWS SDK and dependencies
echo "Installing the AWS GO SDK..."
# Run GO SDK twice to ensure it installs ok
go get github.com/aws/aws-sdk-go > /dev/null 2>&1
go get github.com/aws/aws-sdk-go
go get github.com/awslabs/aws-go-wordfreq-sample

# Create the DynamoDB table
echo "Creating DynamoDB table 'wordfreq'..."
go run ./cmd/createTable/main.go wordfreq

# Build the worker application and install at bin/application
echo "Building the worker application at bin/application..."
rm -f ./bin/application > /dev/null 2>&1
go build -o bin/application ./cmd/worker
if [ -f ./bin/application ]; then echo "Setup complete."; fi
echo
