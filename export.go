package main

import (
	// "fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var (
	uploadDuration          prometheus.Gauge
	uploadStatus            prometheus.Gauge
	uploadError             *prometheus.GaugeVec
	multipartUploadDuration prometheus.Gauge
	multipartUploadStatus   prometheus.Gauge
	multipartUploadError    *prometheus.GaugeVec
	downloadDuration        prometheus.Gauge
	downloadStatus          prometheus.Gauge
	downloadError           *prometheus.GaugeVec
	deleteDuration          prometheus.Gauge
	deleteStatus            prometheus.Gauge
	deleteError             *prometheus.GaugeVec
	restoreDuration         prometheus.Gauge
	restoreStatus           prometheus.Gauge
	restoreError            *prometheus.GaugeVec
)

func loadMetricsReporter(namespace string) {
	downloadDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "download_duration_seconds",
			Help:      "Last download duration in seconds",
		})
	uploadDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upload_duration_seconds",
			Help:      "Last upload duration in seconds",
		})
	multipartUploadDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "multipart_upload_duration_seconds",
			Help:      "Last multipart upload duration in seconds",
		})
	deleteDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "delete_duration_seconds",
			Help:      "Last delete duration in seconds",
		})
	restoreDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "restore_duration_seconds",
			Help:      "Last restore duration in seconds",
		})
	downloadStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "download_status",
			Help:      "Last download status, 1 is ok",
		})
	uploadStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upload_status",
			Help:      "Last upload status, 1 is ok",
		})
	multipartUploadStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "multipart_upload_status",
			Help:      "Last multipart upload status, 1 is ok",
		})
	deleteStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "delete_status",
			Help:      "Last delete status, 1 is ok",
		})
	restoreStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "restore_status",
			Help:      "Last restore status, 1 is ok",
		})
	downloadError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "download_errors",
			Help:      "Active download errors",
		}, []string{"error"},
	)
	uploadError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upload_errors",
			Help:      "Active upload errors",
		}, []string{"error"},
	)
	multipartUploadError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "multipart_upload_errors",
			Help:      "Active multipart upload errors",
		}, []string{"error"},
	)
	deleteError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "delete_errors",
			Help:      "Active delete errors",
		}, []string{"error"},
	)
	restoreError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "restore_errors",
			Help:      "Active restore errors",
		}, []string{"error"},
	)
}

// RecordMetrics -
func RecordMetrics(manager *Manager) {
	go func() {
		for {
			if err := manager.newSession(); err != nil {
				log.Fatalf("failed to create S3 session: %s", err)
			}
			uploadError.Reset()
			start := time.Now()
			if err := manager.Upload(); err != nil {
				uploadError.With(prometheus.Labels{
					"error": err.Error(),
				}).Set(1)
				uploadStatus.Set(0)
			} else {
				uploadStatus.Set(1)
				uploadDuration.Set(time.Since(start).Seconds())
			}

			if manager.config.S3.EnableMultipartUploadCheck {
				multipartUploadError.Reset()
				start = time.Now()
				if err := manager.MultipartUpload(); err != nil {
					multipartUploadError.With(prometheus.Labels{
						"error": err.Error(),
					}).Set(1)
					multipartUploadStatus.Set(0)
				} else {
					multipartUploadStatus.Set(1)
					multipartUploadDuration.Set(time.Since(start).Seconds())
				}
			}

			// Validate versionning
			if manager.config.S3.EnableVersionningCheck {
				// First we delete the related object and retrieve the versionId
				deleteError.Reset()
				start = time.Now()
				versionId, err := manager.DeleteObject(manager.config.S3.DownloadKey)
				if err != nil {
					deleteError.With(prometheus.Labels{
						"error": err.Error(),
					}).Set(0)
					deleteStatus.Set(0)
				} else {
					deleteStatus.Set(1)
					deleteDuration.Set(time.Since(start).Seconds())
				}

				// Then we try to restore the object using the versionId
				restoreError.Reset()
				start = time.Now()
				err = manager.RestoreObject(manager.config.S3.DownloadKey, versionId)
				if err != nil {
					restoreError.With(prometheus.Labels{
						"error": err.Error(),
					}).Set(0)
					restoreStatus.Set(0)
				} else {
					restoreStatus.Set(1)
					restoreDuration.Set(time.Since(start).Seconds())
				}
			}

			downloadError.Reset()
			start = time.Now()
			if err := manager.Download(); err != nil {
				downloadError.With(prometheus.Labels{
					"error": err.Error(),
				}).Set(1)
				downloadStatus.Set(0)
			} else {
				downloadStatus.Set(1)
				downloadDuration.Set(time.Since(start).Seconds())
			}

			time.Sleep(manager.config.Exporter.IntervalDuration)
		}
	}()
}

// Local Variables:
// ispell-local-dictionary: "american"
// End:
