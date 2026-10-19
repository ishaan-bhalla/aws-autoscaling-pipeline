package wordfreq

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

// NewSession returns an AWS session. When AWS_ENDPOINT_URL is set, every
// service (S3, SQS, DynamoDB) is pointed at that endpoint instead of AWS, so
// the same binary runs against the local Moto emulator in docker compose.
func NewSession() *session.Session {
	cfg := aws.NewConfig()
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		cfg = cfg.WithEndpoint(endpoint).WithS3ForcePathStyle(true)
	}
	return session.Must(session.NewSession(cfg))
}
