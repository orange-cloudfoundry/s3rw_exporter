package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
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
	session      *session.Session
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

func (m *Manager) newSession() error {
	m.entry.Debugf("creating new session")

	config := aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(
			m.config.S3.APIKey,
			m.config.S3.APISecret,
			"",
		)).
		WithRegion(m.config.S3.Region).
		WithEndpoint(m.config.S3.URL).
		WithMaxRetries(3).
		WithS3ForcePathStyle(m.config.S3.S3ForcePathStyle)

	sess, err := session.NewSession(config)
	if err != nil {
		m.entry.Errorf("failed to create session: %s", err)
		return fmt.Errorf("failed to create session: %w", err)
	}

	m.session = sess
	m.entry.Debugf("session created successfully")

	return nil
}

func (m *Manager) Download() error {
	m.entry.Debugf("starting download file : %s", m.config.S3.DownloadKey)
	buffer := []byte{}
	memWriter := aws.NewWriteAtBuffer(buffer)
	downloader := s3manager.NewDownloader(m.session)
	_, err := downloader.Download(memWriter,
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
	m.entry.Debugf("file %s has been downloaded successfully", m.config.S3.DownloadKey)
	return nil
}

func (m *Manager) Upload() error {
	m.entry.Debugf("starting upload file : %s", m.config.S3.DownloadKey)
	reader := bytes.NewReader(m.uploadFile)
	uploader := s3manager.NewUploader(m.session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.UploadKey),
	})

	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return fmt.Errorf("unable to upload file: %s", err)
	}

	m.entry.Debugf("file %s has been uploaded successfully", m.config.S3.UploadKey)
	return nil
}

func (m *Manager) MultipartUpload() error {
	m.entry.Debugf("starting multi-part upload for file: %s", m.config.S3.UploadKey)

	reader := bytes.NewReader(m.uploadFile)
	uploader := s3manager.NewUploader(m.session, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // 5 MB part size
		u.Concurrency = 5            // Number of concurrent goroutines for uploading
	})

	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.UploadKey),
	})

	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return fmt.Errorf("unable to upload file: %s", err)
	}

	m.entry.Debugf("file %s has been uploaded successfully", m.config.S3.UploadKey)
	return nil
}

func (m *Manager) DeleteObject(object string) (string, error) {
	m.entry.Infof("deleting object: %s from bucket: %s", object, m.config.S3.Bucket)

	svc := s3.New(m.session)
	listOutput, err := svc.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket: aws.String(m.config.S3.Bucket),
		Prefix: aws.String(object),
	})
	if err != nil {
		m.entry.Errorf("unable to list object versions: %s", err.Error())
		return "", fmt.Errorf("unable to list object versions: %w", err)
	}

	var currentVersionID string
	if len(listOutput.Versions) > 0 {
		currentVersionID = *listOutput.Versions[0].VersionId
		m.entry.Infof("current version ID of object %s is %s", object, currentVersionID)
	} else {
		m.entry.Warnf("no versions found for object %s, it may not be versioned", object)
	}

	deleteOutput, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		m.entry.Errorf("unable to delete object: %s", err.Error())
		return "", fmt.Errorf("unable to delete object: %w", err)
	}

	if deleteOutput.VersionId != nil {
		m.entry.Infof("object %s deleted successfully, delete marker version ID: %s", object, *deleteOutput.VersionId)
	} else {
		m.entry.Infof("object %s deleted successfully (no delete marker version ID returned)", object)
	}

	return currentVersionID, nil
}

func (m *Manager) RestoreObject(object, versionID string) error {
	m.entry.Infof("restoring object: %s with version: %s in bucket: %s", object, versionID, m.config.S3.Bucket)

	// Properly format the CopySource by encoding only the bucket and key
	copySource := fmt.Sprintf("%s/%s?versionId=%s",
		url.PathEscape(m.config.S3.Bucket),
		url.PathEscape(object),
		versionID)

	m.entry.Infof("Restoring with the following URL: %s", copySource)

	// Copy the object from a previous version to restore it
	_, err := s3.New(m.session).CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(m.config.S3.Bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(object),
	})
	if err != nil {
		m.entry.Errorf("unable to restore object: %s", err.Error())
		return fmt.Errorf("unable to restore object: %w", err)
	}

	m.entry.Infof("object %s restored successfully", object)
	return nil
}

func (m *Manager) FirstRun() error {
	client := s3.New(m.session)
	m.entry.Infof("creating bucket '%s'", m.config.S3.Bucket)
	_, err := client.CreateBucket(
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
	uploader := s3manager.NewUploader(m.session)
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
