package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
	"runtime"
	"strconv"

	wordfreq "github.com/ishaan-bhalla/aws-autoscaling-pipeline/app"
)

const defaultMessageVisibilityTimeout = 60

var defaultWorkerCount = runtime.NumCPU()

// A Config provides a collection of configuration values the service will use
// to setup its components.
type Config struct {
	Session *session.Session

	// SQS queue URL job messages will be available at
	WorkerQueueURL string
	// SQS queue URL job results will be written to
	ResultQueueURL string
	// DynamoDB tablename results will be recorded to
	ResultTableName string
	// Number of workers in the worker pool
	NumWorkers int
	// The amount of time in seconds a read job message from the SQS will be
	// hidden from other readers of the queue.
	MessageVisibilityTimeout int64

	// Hardened turns on the failure-handling changes made after the course
	// (WORKER_HARDENED=1). Off by default, so the binary behaves exactly as
	// it did on AWS unless asked otherwise:
	//   - receive a message only when a worker is free (no prefetching), so
	//     each container holds at most NumWorkers messages;
	//   - on SIGTERM, stop receiving, finish the job in hand, then exit;
	//   - write results with a conditional put keyed on the file and its
	//     ETag, so a redelivered message is recorded once;
	//   - leave unparseable messages on the queue for the dead-letter queue
	//     instead of deleting them.
	Hardened bool

	// CrashEvery injects worker crashes for failure testing
	// (WORKER_CRASH_EVERY=N, 0 = off). On a message's first delivery, a file
	// whose name hashes to 0 mod N crashes the worker after counting but
	// before the DynamoDB write; one that hashes to 1 mod N crashes it after
	// the write but before the message is deleted.
	CrashEvery uint32
}

// getConfig collects the configuration from the environment variables, and
// returns it, or error if it was unable to collect the configuration.
func getConfig() (Config, error) {
	c := Config{
		WorkerQueueURL:  os.Getenv("WORKER_QUEUE_URL"),
		ResultQueueURL:  os.Getenv("WORKER_RESULT_QUEUE_URL"),
		ResultTableName: os.Getenv("WORKER_RESULT_TABLENAME"),
		Session:         wordfreq.NewSession(),
	}

	if c.WorkerQueueURL == "" {
		return c, fmt.Errorf("missing WORKER_QUEUE_URL")
	}
	if c.ResultQueueURL == "" {
		return c, fmt.Errorf("missing WORKER_RESULT_QUEUE_URL")
	}
	if c.ResultTableName == "" {
		return c, fmt.Errorf("missing WORKER_RESULT_TABLENAME")
	}

	if aws.StringValue(c.Session.Config.Region) == "" {
		region, err := ec2metadata.New(c.Session).Region()
		if err != nil {
			return c, fmt.Errorf("region not specified, unable to retrieve from EC2 instance %v", err)
		}
		c.Session.Config.Region = aws.String(region)
	}

	if timeoutStr := os.Getenv("WORKER_MESSAGE_VISIBILITY"); timeoutStr != "" {
		timeout, err := strconv.ParseInt(timeoutStr, 10, 64)
		if err != nil {
			return c, err
		}
		if timeout <= 0 {
			return c, fmt.Errorf("invalid message visibility timeout")
		}
		c.MessageVisibilityTimeout = timeout
	} else {
		c.MessageVisibilityTimeout = defaultMessageVisibilityTimeout
	}

	atOnceStr := os.Getenv("WORKER_COUNT")
	if atOnceStr == "" {
		c.NumWorkers = defaultWorkerCount
	} else {
		atOnce, err := strconv.ParseInt(atOnceStr, 10, 64)
		if err != nil {
			return c, err
		}
		if atOnce <= 0 {
			return c, fmt.Errorf("invalid worker number")
		}
		c.NumWorkers = int(atOnce)
	}

	c.Hardened = os.Getenv("WORKER_HARDENED") == "1"

	if s := os.Getenv("WORKER_CRASH_EVERY"); s != "" {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return c, fmt.Errorf("invalid WORKER_CRASH_EVERY %v", err)
		}
		c.CrashEvery = uint32(n)
	}

	return c, nil
}
