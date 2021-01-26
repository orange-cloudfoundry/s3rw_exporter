package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

type manager struct {
	config       *Config
	downloadFile []byte
	uploadFile   []byte
	entry        *log.Entry
}

// NewManager -
func NewManager(config *Config) (*manager, error) {
	download, err := ioutil.ReadFile(config.S3.DownloadFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable read configured download file from path %s", config.S3.DownloadFilePath)
	}
	upload, err := ioutil.ReadFile(config.S3.UploadFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable read configured upload file from path %s", config.S3.UploadFilePath)
	}
	return &manager{
		config:       config,
		downloadFile: download,
		uploadFile:   upload,
		entry: log.WithFields(log.Fields{
			"url":    config.S3.URL,
			"bucket": config.S3.Bucket,
		}),
	}, nil
}

func (m *manager) newSession() (*session.Session, error) {
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
	return session.NewSession(config)
}

func (m *manager) Download() error {
	session, err := m.newSession()
	if err != nil {
		m.entry.Errorf("unable to create session: %s", err.Error())
		return errors.Wrap(err, "unable to create session")
	}

	buffer := []byte{}
	memWriter := aws.NewWriteAtBuffer(buffer)
	downloader := s3manager.NewDownloader(session)
	_, err = downloader.Download(memWriter,
		&s3.GetObjectInput{
			Bucket: aws.String(m.config.S3.Bucket),
			Key:    aws.String(m.config.S3.DownloadKey),
		})

	if err != nil {
		m.entry.Errorf("unable to download file: %s", err.Error())
		return errors.Wrap(err, "unable to download file")
	}
	if !bytes.Equal(m.downloadFile, memWriter.Bytes()) {
		m.entry.Errorf("downloaded file content mismatch")
		return errors.New("downloaded file content mismatch")
	}
	return nil
}

func (m *manager) Upload() error {
	session, err := m.newSession()
	if err != nil {
		m.entry.Errorf("unable to create session: %s", err.Error())
		return errors.Wrap(err, "unable to create session")
	}

	reader := bytes.NewReader(m.uploadFile)
	uploader := s3manager.NewUploader(session)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.UploadKey),
	})

	if err != nil {
		m.entry.Errorf("unable to upload file: %s", err.Error())
		return errors.Wrap(err, "unable to upload file")
	}

	return nil
}

func (m *manager) FirstRun() error {
	session, err := m.newSession()
	if err != nil {
		return errors.Wrap(err, "unable to create session")
	}

	client := s3.New(session)
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
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				m.entry.Warnf("bucket already exists: %s", err.Error())
			default:
				return errors.Wrapf(err, "unable to create bucket '%s'", m.config.S3.Bucket)
			}
		} else {
			return errors.Wrapf(err, "unexpected error while creating bucket '%s'", m.config.S3.Bucket)
		}
	}

	reader := bytes.NewReader(m.downloadFile)
	uploader := s3manager.NewUploader(session)
	m.entry.Infof("uploading initial file '%s' from '%s'", m.config.S3.DownloadKey, m.config.S3.DownloadFilePath)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(m.config.S3.Bucket),
		Key:    aws.String(m.config.S3.DownloadKey),
	})
	if err != nil {
		return errors.Wrap(err, "unable to upload initial file")
	}

	return nil
}

// Local Variables:
// ispell-local-dictionary: "american"
// End:
