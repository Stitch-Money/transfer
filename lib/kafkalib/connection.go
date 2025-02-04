package kafkalib

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Mechanism string

const (
	Plain       Mechanism = "PLAIN"
	ScramSha512 Mechanism = "SCRAM-SHA-512"
	AwsMskIam   Mechanism = "AWS-MSK-IAM"
	SaslPlain   Mechanism = "SASL-PLAIN"
)

type Connection struct {
	enableAWSMSKIAM bool
	disableTLS      bool
	username        string
	password        string
	saslMechanism   string
}

func NewConnection(enableAWSMSKIAM bool, disableTLS bool, username, password string) Connection {

	return Connection{
		enableAWSMSKIAM: enableAWSMSKIAM,
		disableTLS:      disableTLS,
		username:        username,
		password:        password,
	}
}

func NewSaslPlainConnection(username string, password string) Connection {
	return Connection{
		username:      username,
		password:      password,
		saslMechanism: string(Plain),
	}
}

func (c Connection) Mechanism() Mechanism {
	if c.saslMechanism == string(Plain) {
		return SaslPlain
	}

	if c.enableAWSMSKIAM {
		return AwsMskIam
	}

	if c.username != "" && c.password != "" {
		return ScramSha512
	}

	return Plain
}

func (c Connection) Dialer(ctx context.Context, awsOptFns ...func(options *awsCfg.LoadOptions) error) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism, err := scram.Mechanism(scram.SHA512, c.username, c.password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}

		dialer.SASLMechanism = mechanism
		if !c.disableTLS {
			dialer.TLS = &tls.Config{}
		}
	case AwsMskIam:
		_awsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load aws configuration: %w", err)
		}

		dialer.SASLMechanism = aws_msk_iam_v2.NewMechanism(_awsCfg)
		// We don't need to disable TLS for AWS IAM since MSK will always enable TLS.
		dialer.TLS = &tls.Config{}
	case SaslPlain:
		mechanism := plain.Mechanism{Username: c.username, Password: c.password}

		dialer.SASLMechanism = mechanism
		if !c.disableTLS {
			dialer.TLS = &tls.Config{}
		}
	case Plain:
		// do nothing
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return dialer, nil
}

func (c Connection) Transport(ctx context.Context, awsOptFns ...func(options *awsCfg.LoadOptions) error) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism, err := scram.Mechanism(scram.SHA512, c.username, c.password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}

		transport.SASL = mechanism
		if !c.disableTLS {
			transport.TLS = &tls.Config{}
		}
	case AwsMskIam:
		_awsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		transport.SASL = aws_msk_iam_v2.NewMechanism(_awsCfg)
		if !c.disableTLS {
			transport.TLS = &tls.Config{}
		}
	case SaslPlain:
		mechanism := plain.Mechanism{Username: c.username, Password: c.password}

		transport.SASL = mechanism
		if !c.disableTLS {
			transport.TLS = &tls.Config{}
		}
	case Plain:
		// do nothing
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return transport, nil
}
