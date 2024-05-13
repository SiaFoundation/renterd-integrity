package main

import (
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	cfg = config{
		BusAddr:  "http://localhost:9880/api/bus",
		BusPassw: "test",

		WorkerAddr:  "http://localhost:9880/api/worker",
		WorkerPassw: "test",

		IntegrityCheckInterval:    time.Hour,
		IntegrityCheckDownloadPct: 1, // 1% every hour
		IntegrityCheckDeletePct:   1, // 1% every hour

		DatasetSize: 10 << 30, // 10 GiB
		MinFilesize: 1 << 20,  // 1 MiB
		MaxFilesize: 1 << 23,  // 8 MiB

		CleanStart: false,
		WorkDir:    "data",
	}
)

type (
	config struct {
		BusAddr  string `yaml:"busAddress"`
		BusPassw string `yaml:"busPassword"`

		WorkerAddr  string `yaml:"workerAddress"`
		WorkerPassw string `yaml:"workerPassword"`

		HealthCheckInterval       time.Duration `yaml:"healthCheckInterval"`
		IntegrityCheckInterval    time.Duration `yaml:"integrityCheckInterval"`
		IntegrityCheckDeletePct   float64       `yaml:"integrityCheckDeletePct"`
		IntegrityCheckDownloadPct float64       `yaml:"integrityCheckDownloadPct"`

		DatasetSize int64 `yaml:"datasetSize"`
		MinFilesize int64 `yaml:"minFilesize"`
		MaxFilesize int64 `yaml:"maxFilesize"`

		CleanStart bool   `yaml:"cleanStart"`
		WorkDir    string `yaml:"workDir"`
	}
)

func (c config) buildTmpFilepath() string {
	_ = os.MkdirAll(filepath.Join(cfg.WorkDir, "tmp"), 0700)
	return filepath.Join(cfg.WorkDir, "tmp", randomString())
}

func (c config) buildHashFilepath(h hash.Hash) string {
	_ = os.MkdirAll(cfg.WorkDir, 0700)
	return filepath.Join(cfg.WorkDir, fmt.Sprintf("%x%s", h.Sum(nil), dataExtension))
}

func loadConfig(path string) error {
	// check whether the config file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	// open the file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open config file at '%s', err: %v", path, err)
	}
	defer f.Close()

	// decode the config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("failed to decode config file, err: %v", err)
	}

	// initialize directory
	err = os.MkdirAll(cfg.WorkDir, 0700)
	if err != nil {
		return fmt.Errorf("failed to create directory '%v', err: %v", cfg.WorkDir, err)
	}

	// TODO: verify config
	return nil
}
