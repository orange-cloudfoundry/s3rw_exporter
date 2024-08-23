package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"gopkg.in/yaml.v2"
)

type logConfig struct {
	JSON  bool   `yaml:"json"`
	Level string `yaml:"level"`
}

type exporterConfig struct {
	IntervalDuration time.Duration `yaml:"interval_duration"`
	Port             int           `yaml:"port"`
	Path             string        `yaml:"path"`
	Namespace        string        `yaml:"namespace"`
}

type s3Config struct {
	URL                        string `yaml:"url"`
	Region                     string `yaml:"region"`
	Bucket                     string `yaml:"bucket"`
	DownloadKey                string `yaml:"download_file_name"`
	DownloadFilePath           string `yaml:"download_file_path"`
	UploadKey                  string `yaml:"upload_file_name"`
	UploadFilePath             string `yaml:"upload_file_path"`
	APIKey                     string `yaml:"api_key"`
	APISecret                  string `yaml:"secret_access_key"`
	EnableVersionningCheck     bool   `yaml:"enable_versionning_check"`
	EnableMultipartUploadCheck bool   `yaml:"enable_multipart_upload_check"`
	EnableLockingObjectCheck   bool   `yaml:"enable_locking_object_check"`
	S3ForcePathStyle           bool   `yaml:"s3_force_path_style"`
}

// Config -
type Config struct {
	Log      logConfig      `yaml:"log"`
	Exporter exporterConfig `yaml:"exporter"`
	S3       s3Config       `yaml:"s3"`
}

func (c *exporterConfig) validate() error {
	if 0 == len(c.Path) {
		return fmt.Errorf("missing key 'exporter.path'")
	}
	if 0 == c.Port {
		return fmt.Errorf("missing or zero key 'expoerter.port'")
	}
	if 0 == c.IntervalDuration {
		return fmt.Errorf("missing or zero key 'exporter.interval_duration'")
	}
	return nil
}

func (c *s3Config) validate() error {
	if len(c.URL) == 0 {
		return fmt.Errorf("missing mandatory key s3.url")
	}
	if len(c.Bucket) == 0 {
		return fmt.Errorf("missing mandatory key s3.bucket")
	}
	if len(c.Region) == 0 {
		return fmt.Errorf("missing mandatory key s3.region")
	}
	if len(c.DownloadKey) == 0 {
		return fmt.Errorf("missing mandatory key s3.download_key")
	}
	if len(c.DownloadFilePath) == 0 {
		return fmt.Errorf("missing mandatory key s3.download_file_path")
	}
	if len(c.UploadKey) == 0 {
		return fmt.Errorf("missing mandatory key s3.upload_key")
	}
	if len(c.UploadFilePath) == 0 {
		return fmt.Errorf("missing mandatory key s3.upload_file_path")
	}
	if len(c.APIKey) == 0 {
		return fmt.Errorf("missing mandatory key s3.api_key")
	}
	if len(c.APISecret) == 0 {
		return fmt.Errorf("missing mandatory key s3.api_secret")
	}
	// Default S3ForcePathStyle to false if not provided
	if !c.S3ForcePathStyle {
		c.S3ForcePathStyle = false
	}

	if !c.EnableVersionningCheck {
		c.EnableVersionningCheck = false
	}

	if !c.EnableMultipartUploadCheck {
		c.EnableMultipartUploadCheck = false
	}

	if !c.EnableLockingObjectCheck {
		c.EnableLockingObjectCheck = false
	}

	return nil
}

// Validate - Validate configuration object
func (c *Config) Validate() error {
	if err := c.S3.validate(); err != nil {
		return fmt.Errorf("invalid s3 configuration: %s", err)
	}
	if err := c.Exporter.validate(); err != nil {
		return fmt.Errorf("invalid exporter configuration: %s", err)
	}
	return nil
}

// NewConfig - Creates and validates config from given reader
func NewConfig(file io.Reader) *Config {
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("unable to read configuration file : %s", err)
		os.Exit(1)
	}
	config := Config{}
	if err = yaml.Unmarshal(content, &config); err != nil {
		log.Fatalf("unable to read configuration yaml file: %s", err)
		if err = json.Unmarshal(content, &config); err != nil {
			log.Fatalf("unable to read configuration json file: %s", err)
			os.Exit(1)
		}
	}
	if err = config.Validate(); err != nil {
		log.Fatalf("invalid configuration, %s", err)
		os.Exit(1)
	}
	return &config
}
