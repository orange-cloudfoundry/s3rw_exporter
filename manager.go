package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/config"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	log "github.com/sirupsen/logrus"
)

type Manager struct {
	config       *Config
	downloadFile []byte
	uploadFile   []byte
	entry        *log.Entry
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
	return &Manager{
		config:       config,
		downloadFile: download,
		uploadFile:   upload,
		entry: log.WithFields(log.Fields{
			"url":    config.S3.URL,
			"bucket": config.S3.Bucket,
		}),
	}, nil
}

func (m *Manager) newConfig(ctx context.Context) (aws.Config, error) {
	m.entry.Debugf("creating new AWS config")
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(m.config.S3.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			m.config.S3.APIKey,
			m.config.S3.APISecret,
			"",
		)),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: m.config.S3.URL,
				}, nil
			}),
		),
	)
	if err != nil {
		return aws.Config{}, err
	}
	return cfg, nil
}

func (m *Manager) Download() error {
	ctx := context.Background()
	cfg, err := m.newConfig(ctx)
	if err != nil {
		m.entry.Errorf("unable to create AWS config: %s", err.Error())
		return fmt.Errorf("unable to create AWS config: %s", err)
	}

	client := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(client)
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err = downloader.Download(ctx, buf, &s3.GetObjectInput{
		Bucket: &m.config.S3.Bucket,
		Key:    &m.config.S3.DownloadKey,
	})
	if err != nil {
		m.entry.Errorf("unable to download file: %s", err.Error())
		return fmt.Errorf("unable to download file: %s", err)
	}
	if !bytes.Equal(m.downloadFile, buf.Bytes()) {
		m.entry.Errorf("downloaded file content mismatch")
		return errors.New("downloaded file content mismatch")
	}
	return nil
}

func (m *Manager) Upload() error {
	ctx := context.Background()
	cfg, err := m.newConfig(ctx)
	if err != nil {
		m.entry.Errorf("unable to create AWS config: %s", err.Error())
		return fmt.Errorf("unable to create AWS config: %s", err)
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)
	reader := bytes.NewReader(m.uploadFile)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Body:   reader,
		Bucket: &m.config.S3.Bucket,
		Key:    &m.config.S3.UploadKey,
	})
	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return fmt.Errorf("unable to upload file: %s", err)
	}
	return nil
}

func (m *Manager) FirstRun() error {
	ctx := context.Background()
	cfg, err := m.newConfig(ctx)
	if err != nil {
		return fmt.Errorf("unable to create AWS config: %s", err)
	}

	client := s3.NewFromConfig(cfg)
	m.entry.Infof("creating bucket '%s'", m.config.S3.Bucket)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &m.config.S3.Bucket,
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(m.config.S3.Region),
		},
	})
	if err != nil {
		var bErr *s3types.BucketAlreadyExists
		var bOwnErr *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &bErr) || errors.As(err, &bOwnErr) {
			m.entry.Warnf("bucket already exists: %s", err.Error())
		} else {
			return fmt.Errorf("unable to create bucket '%s': %s", m.config.S3.Bucket, err)
		}
	}

	reader := bytes.NewReader(m.downloadFile)
	uploader := manager.NewUploader(client)
	m.entry.Infof("uploading initial file '%s' from '%s'", m.config.S3.DownloadKey, m.config.S3.DownloadFilePath)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Body:   reader,
		Bucket: &m.config.S3.Bucket,
		Key:    &m.config.S3.DownloadKey,
	})
	if err != nil {
		return fmt.Errorf("unable to upload initial file: %s", err)
	}
	return nil
}
