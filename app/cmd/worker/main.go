package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"

	wordfreq "github.com/ishaan-bhalla/aws-autoscaling-pipeline/app"
)

// Worker service which reads from an SQS queue pulls off job messages, processes
// the jobs, and records the results. The service uses environment variables for
// its configuration.
//
// Requires the following environment variables to be set.
//
// * WORKER_QUEUE_URL - The SQS queue URL where the service will read job messages
// from. Job messages are created when S3 notifies the SQS queue that a file has
// been uploaded to a particular bucket.
//
// * WORKER_RESULT_QUEUE_URL - The SQS queue URL where the job results will be
// sent to.
//
// * WORKER_RESULT_TABLENAME - The name of the DynamoDB table result items should
// be recorded to.
//
// Optionally the follow environment variables can be provided.
//
// * AWS_REGION - The AWS region the worker will use for signing and making all
// requests to. This parameter is only optional if the service is running within
// an EC2 instance. If not running in an EC2 instance AWS_REGION is required.
//
// * WORKER_MESSAGE_VISIBILITY - The ammount of time messges will be hidden in
// the SQS job message queue from other services when a service reads that message.
// Will also be used to extend the visibility timeout for long running jobs.
// Defaults to 60s.
//
// * WORKER_COUNT - The number of workers in the worker pool. Defaults to the
// number of virtual CPUs in the system.
func main() {
	cfg, err := getConfig()
	if err != nil {
		log.Println("Unable to get config", err)
		os.Exit(1)
	}

	// The original service only handled Ctrl+C, so a SIGTERM (an instance
	// or container being stopped) killed it with jobs in hand. Hardened
	// mode treats SIGTERM the same way: stop receiving, finish, exit.
	signals := []os.Signal{os.Interrupt}
	if cfg.Hardened {
		signals = append(signals, syscall.SIGTERM)
	}
	doneCh := listenForSignals(signals...)

	sqsSvc := sqs.New(cfg.Session)
	queue := NewJobMessageQueue(cfg.WorkerQueueURL, cfg.MessageVisibilityTimeout, 5, sqsSvc)
	if cfg.Hardened {
		queue.Bound(cfg.NumWorkers)
	}
	go queue.Listen(doneCh)

	// Job Workers
	resultsCh := make(chan *wordfreq.JobResult, 10)
	workers := NewWorkerPool(cfg.NumWorkers, resultsCh, queue, s3.New(cfg.Session), cfg.CrashEvery)

	// Notifier to send a message to an Amazon SQS Queue
	notify := NewResultNotifier(sqsSvc, cfg.ResultQueueURL)
	// Recorder to write results to Amazon DynamoDB
	recorder := NewResultRecorder(cfg.ResultTableName, dynamodb.New(cfg.Session))
	recorder.idempotent = cfg.Hardened

	// Job Progress Collector
	collector := NewResultCollector(notify, recorder, queue)
	collector.crashEvery = cfg.CrashEvery
	go collector.ProcessJobResult(resultsCh)

	// Wait for the workers to complete before continuing on to exit
	workers.WaitForWorkersDone()
	close(resultsCh)

	// Wait for all results to be completed before continuing
	collector.WaitForResults()
	fmt.Println("Worker service exited cleanly")
}

// Handle Ctr+C / Sig Interrupt (and SIGTERM in hardened mode)
func listenForSignals(sigs ...os.Signal) <-chan struct{} {
	doneCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, sigs...)
	go func() {
		closed := false
		for sig := range sigCh {
			if !closed {
				fmt.Printf("Received %s, existing...\n", sig)
				closed = true
				close(doneCh)
			}
		}
	}()

	return doneCh
}
