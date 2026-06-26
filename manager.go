package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
)

type Manager struct {
	config       *Config
	downloadFile []byte
	uploadFile   []byte
	entry        *log.Entry
	client       *s3.Client
	tmClient     *transfermanager.Client
}

// NewManager -
func NewManager(config *Config) (*Manager, error) {
	download, err := os.ReadFile(config.S3.DownloadFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable read configured download file from path '%s': %s", config.S3.DownloadFilePath, err)
	}
	upload, err := os.ReadFile(config.S3.UploadFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable read configured upload file from path '%s': %s", config.S3.UploadFilePath, err)
	}

	mgr := &Manager{
		config:       config,
		downloadFile: download,
		uploadFile:   upload,
		entry: log.WithFields(log.Fields{
			"url":    config.S3.URL,
			"bucket": config.S3.Bucket,
		}),
	}

	// Initialize S3 client
	client, err := mgr.newClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to create S3 client: %w", err)
	}
	mgr.client = client
	mgr.tmClient = transfermanager.New(client)

	return mgr, nil
}

func (m *Manager) newClient(ctx context.Context) (*s3.Client, error) {
	m.entry.Debugf("creating new S3 client")

	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion(m.config.S3.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				m.config.S3.APIKey,
				m.config.S3.APISecret,
				"",
			),
		),
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var clientOpts []func(*s3.Options)

	if m.config.S3.URL != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(m.config.S3.URL)
			o.UsePathStyle = true
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
			o.DisableLogOutputChecksumValidationSkipped = true
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)
	return client, nil
}

func (m *Manager) Download() error {
	ctx := context.Background()

	buffer := types.NewWriteAtBuffer(make([]byte, 0))
	_, err := m.tmClient.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
		Bucket:   aws.String(m.config.S3.Bucket),
		Key:      aws.String(m.config.S3.DownloadKey),
		WriterAt: buffer,
	})

	if err != nil {
		m.entry.Errorf("unable to download file: %s", err.Error())
		return fmt.Errorf("unable to download file: %w", err)
	}

	if !bytes.Equal(m.downloadFile, buffer.Bytes()) {
		m.entry.Errorf("downloaded file content mismatch")
		return errors.New("downloaded file content mismatch")
	}
	return nil
}

func (m *Manager) Upload() error {
	ctx := context.Background()

	reader := bytes.NewReader(m.uploadFile)

	// Remove potential leading slash from upload key
	key := m.config.S3.UploadKey
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	m.entry.Debugf("uploading file: %s to bucket %s", key, m.config.S3.Bucket)

	_, err := m.tmClient.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return fmt.Errorf("unable to upload file: %w", err)
	}

	return nil
}

func (m *Manager) FirstRun() error {
	ctx := context.Background()

	m.entry.Infof("creating bucket '%s'", m.config.S3.Bucket)
	_, err := m.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(m.config.S3.Bucket),
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(m.config.S3.Region),
		},
	})

	if err != nil {
		var bucketAlreadyExists *s3types.BucketAlreadyExists
		var bucketAlreadyOwnedByYou *s3types.BucketAlreadyOwnedByYou

		if errors.As(err, &bucketAlreadyExists) || errors.As(err, &bucketAlreadyOwnedByYou) {
			m.entry.Warnf("bucket already exists: %s", err.Error())
		} else {
			return fmt.Errorf("unable to create bucket '%s': %w", m.config.S3.Bucket, err)
		}
	}

	reader := bytes.NewReader(m.downloadFile)

	m.entry.Infof("uploading initial file '%s' from '%s'", m.config.S3.DownloadKey, m.config.S3.DownloadFilePath)
	_, err = m.tmClient.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.DownloadKey),
	})

	if err != nil {
		return fmt.Errorf("unable to upload initial file: %w", err)
	}

	return nil
}
