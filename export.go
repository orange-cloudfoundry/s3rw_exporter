package main

import (
	// "fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

var (
	uploadDuration   prometheus.Gauge
	uploadStatus     prometheus.Gauge
	uploadError      *prometheus.GaugeVec
	downloadDuration prometheus.Gauge
	downloadStatus   prometheus.Gauge
	downloadError    *prometheus.GaugeVec
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
}

// RecordMetrics -
func RecordMetrics(manager *manager) {
	go func() {
		for {
			downloadError.Reset()
			start := time.Now()
			if err := manager.Download(); err != nil {
				downloadError.With(prometheus.Labels{
					"error": err.Error(),
				}).Set(1)
				downloadStatus.Set(0)
			} else {
				downloadStatus.Set(1)
				downloadDuration.Set(time.Since(start).Seconds())
			}

			uploadError.Reset()
			start = time.Now()
			if err := manager.Upload(); err != nil {
				uploadError.With(prometheus.Labels{
					"error": err.Error(),
				}).Set(1)
				uploadStatus.Set(0)
			} else {
				uploadStatus.Set(1)
				uploadDuration.Set(time.Since(start).Seconds())
			}
			time.Sleep(manager.config.Exporter.IntervalDuration)
		}
	}()
}

// Local Variables:
// ispell-local-dictionary: "american"
// End:
