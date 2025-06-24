package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

func (m *Manager) newSession() (*session.Session, error) {
	m.entry.Debugf("creating new session")

	config := aws.NewConfig()
	config.WithCredentials(credentials.NewStaticCredentials(
		m.config.S3.APIKey,
		m.config.S3.APISecret,
		"",
	))
	config.WithRegion(m.config.S3.Region)
	config.WithEndpoint(m.config.S3.URL)
	config.WithMaxRetries(3)
	config.WithS3ForcePathStyle(*aws.Bool(m.config.S3.S3ForcePathStyle))
	return session.NewSession(config)
}

func (m *Manager) Download() error {
	newSession, err := m.newSession()
	if err != nil {
		m.entry.Errorf("unable to create session: %s", err.Error())
		return fmt.Errorf("unable to create session: %s", err)
	}

	buffer := []byte{}
	memWriter := aws.NewWriteAtBuffer(buffer)
	downloader := s3manager.NewDownloader(newSession)
	_, err = downloader.Download(memWriter,
		&s3.GetObjectInput{
			Bucket: aws.String(m.config.S3.Bucket),
			Key:    aws.String(m.config.S3.DownloadKey),
		})

	if err != nil {
		m.entry.Errorf("unable to download file: %s", err.Error())
		return fmt.Errorf("unable to download file: %s", err)
	}
	if !bytes.Equal(m.downloadFile, memWriter.Bytes()) {
		m.entry.Errorf("downloaded file content mismatch")
		return errors.New("downloaded file content mismatch")
	}
	return nil
}

func (m *Manager) Upload() error {
	newSession, err := m.newSession()
	if err != nil {
		m.entry.Errorf("unable to create session: %s", err.Error())
		return fmt.Errorf("unable to create session: %s", err)
	}

	reader := bytes.NewReader(m.uploadFile)
	uploader := s3manager.NewUploader(newSession)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.UploadKey),
	})

	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return fmt.Errorf("unable to upload file: %s", err)
	}

	return nil
}

func (m *Manager) FirstRun() error {
	newSession, err := m.newSession()
	if err != nil {
		return fmt.Errorf("unable to create session: %s", err)
	}

	client := s3.New(newSession)
	m.entry.Infof("creating bucket '%s'", m.config.S3.Bucket)
	_, err = client.CreateBucket(
		&s3.CreateBucketInput{
			Bucket: aws.String(m.config.S3.Bucket),
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: aws.String(m.config.S3.Region),
			},
		},
	)
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				m.entry.Warnf("bucket already exists: %s", err.Error())
			default:
				return fmt.Errorf("unable to create bucket '%s': %s", m.config.S3.Bucket, err)
			}
		}
	}

	reader := bytes.NewReader(m.downloadFile)
	uploader := s3manager.NewUploader(newSession)
	m.entry.Infof("uploading initial file '%s' from '%s'", m.config.S3.DownloadKey, m.config.S3.DownloadFilePath)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.DownloadKey),
	})
	if err != nil {
		return fmt.Errorf("unable to upload initial file: %s", err)
	}

	return nil
}
