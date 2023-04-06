package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/prometheus/common/version"
	"github.com/alecthomas/kingpin/v2"
	"net/http"
	"os"
	"strconv"
)

var (
	configFile = kingpin.Flag("config", "Configuration file path").Required().File()
	firstRun   = kingpin.Flag("first-run", "initialize bucket and upload file expected by download check").Bool()
)

func main() {
	kingpin.Version(version.Print("s3rw"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.SetOutput(os.Stderr)
	log.SetLevel(log.ErrorLevel)

	config := NewConfig(*configFile)
	if lvl, err := log.ParseLevel(config.Log.Level); err == nil {
		log.SetLevel(lvl)
	}
	if config.Log.JSON {
		log.SetFormatter(&log.JSONFormatter{})
	}

	manager, err := NewManager(config)
	if err != nil {
		panic(err)
	}
	if *firstRun {
		if err = manager.FirstRun(); err != nil {
			log.Fatal(err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	namespace := "s3rw"
	if config.Exporter.Namespace != "" {
		namespace = config.Exporter.Namespace
	}
	loadMetricsReporter(namespace)
	RecordMetrics(manager)
	http.Handle(manager.config.Exporter.Path, promhttp.Handler())
	addr := ":" + strconv.Itoa(manager.config.Exporter.Port)
	log.Infof("listening on %s", addr)
	http.ListenAndServe(addr, nil)
}
