package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	wordfreq "github.com/ishaan-bhalla/aws-autoscaling-pipeline/app"
)

// A ResultRecorder provides the an abstraction to record job results to DynamoDB.
type ResultRecorder struct {
	tableName string
	svc       dynamodbiface.DynamoDBAPI
	// idempotent makes the write conditional on the file (and its ETag) not
	// being recorded already (hardened mode).
	idempotent bool
}

// ErrAlreadyRecorded is returned by Record when an idempotent write finds the
// same file version already in the table: a redelivered message whose first
// delivery was recorded before the worker died. It is safe to delete.
var ErrAlreadyRecorded = errors.New("result already recorded")

// NewResultRecorder creates a new instance of the ResultRecorder configured
// with a DynamoDB service client.
func NewResultRecorder(tableName string, svc dynamodbiface.DynamoDBAPI) *ResultRecorder {
	return &ResultRecorder{
		tableName: tableName,
		svc:       svc,
	}
}

// Record marshals the job result into a dynamodb.AttributeValue struct, and writes
// the result item to DyanmoDB.
func (r *ResultRecorder) Record(result *wordfreq.JobResult) error {
	// Construct a result item representing what data we want to write to DynamoDB.
	recordItem := resultRecord{
		Filename:       path.Join(result.Job.Bucket, result.Job.Key),
		Words:          map[string]int{},
		ETag:           result.Job.ETag,
		EnqueuedAtMs:   result.Job.OrigMessage.SentTimestampMs,
		RecordedAtMs:   time.Now().UnixMilli(),
		ReceiveCount:   result.Job.OrigMessage.ReceiveCount,
		RecordedByHost: hostname,
	}
	for _, w := range result.Words {
		recordItem.Words[w.Word] = w.Count
	}

	// Use the ConvertToX helpers to marshal a Go struct to a dyanmodb.AttributeValue
	// type. This greatly simplifies the code needed to create the attribute
	// value item.
	av, err := dynamodbattribute.ConvertToMap(recordItem)
	if err != nil {
		return fmt.Errorf("unable to serialize result to dyanmoDB.AttributeValue, %v", err)
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      av,
	}
	if r.idempotent {
		// Write only if this file has not been recorded, or was recorded
		// from a different version of the object (a genuine re-upload).
		input.ConditionExpression = aws.String("attribute_not_exists(Filename) OR ETag <> :etag")
		input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":etag": {S: aws.String(result.Job.ETag)},
		}
	}
	_, err = r.svc.PutItem(input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		return ErrAlreadyRecorded
	}
	if err != nil {
		return fmt.Errorf("unable to record result, %v", err)
	}

	return nil
}

// a resultRecord represents the result item in DynamoDB.
type resultRecord struct {
	Filename string // Table hash key
	Words    map[string]int

	// Instrumentation for the local experiments (also harmless on AWS).
	ETag           string
	EnqueuedAtMs   int64 // SQS SentTimestamp of the job message
	RecordedAtMs   int64 // when this write was made
	ReceiveCount   int   // SQS ApproximateReceiveCount of the delivery recorded
	RecordedByHost string
}

var hostname, _ = os.Hostname()
