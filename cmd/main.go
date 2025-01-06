package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

const (
	defaultBucketName = "integrity"
	defaultConfigFile = "config.yml"
	defaultLogFile    = "checker.log"
	defaultStateFile  = "integrity.json"
)

var (
	bc     *bus.Client
	wc     *worker.Client
	rs     api.RedundancySettings
	logger *zap.SugaredLogger

	errIntegrity = errors.New("integrity check failed")
)

func main() {
	// load config
	err := loadConfig(defaultConfigFile)
	if err != nil {
		log.Fatal(err)
	}

	// initialize logger
	l, closeFn, err := newLogger(defaultLogFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = withSaneTimeout(func(ctx context.Context) error { return closeFn(ctx) }, nil) }()
	logger = l.Sugar().Named("integrity")

	// initialize bus client
	bc = bus.NewClient(cfg.BusAddr, cfg.BusPassw)
	if _, err := bc.State(); err != nil {
		logger.Fatalf("failed to fetch bus state, err: %v", err)
	}

	// initialize worker client
	wc = worker.NewClient(cfg.WorkerAddr, cfg.WorkerPassw)
	if _, err := wc.State(); err != nil {
		logger.Fatalf("failed to fetch worker state, err: %v", err)
	}

	// load state
	s, err := loadState(defaultStateFile)
	if err != nil {
		logger.Fatal(err)
	}

	// ensure bucket exists
	err = bc.CreateBucket(context.Background(), defaultBucketName, api.CreateBucketOptions{})
	if err != nil {
		logger.Fatal(err)
	}

	// remove all files
	if cfg.CleanStart {
		logger.Infof("remove all files from %s/", cfg.WorkDir)
		if err := withSaneTimeout(func(ctx context.Context) error {
			return bc.RemoveObjects(ctx, defaultBucketName, cfg.WorkDir)
		}, nil); err != nil && !strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
			logger.Fatal(err)
		}

		logger.Infof("resetting state")
		s = &state{}
	}

	// run the integrity checks
	stopChan := make(chan struct{})
	defer close(stopChan)
	go run(cfg, s, stopChan)

	// listen for interrupt signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	logger.Info("Shutting down...")
}

func run(cfg config, s *state, stopChan chan struct{}) {
	ticker := time.NewTicker(cfg.IntegrityCheckInterval)
	for {
		if s.timeSinceLastIntegrityCheck() > cfg.IntegrityCheckInterval {
			res := runIntegrityChecks()
			if err := registerAlert(res); err != nil {
				logger.Warnf("failed to register alert, err: %v", err)
			}

			s.Results = append([]result{res}, s.Results...)
			if err := saveState(s, defaultStateFile); err != nil {
				logger.Errorf("failed to save state, err: %v", err)
			}
		} else {
			logger.Debugf("skipping integrity check, it hasn't been %v since the last check", cfg.IntegrityCheckInterval)
		}

		select {
		case <-stopChan:
			return
		case <-ticker.C:
		}
	}
}

func runIntegrityChecks() (res result) {
	logger.Info("running integrity checks")

	// defer building the result
	var err error
	var uploaded, downloaded, removed, prunable int64
	var downloadedMBPS, uploadedMBPS float64
	var complete bool
	defer func(start time.Time) {
		res = result{
			StartedAt: start.UTC(),
			EndedAt:   time.Now().UTC(),

			Uploaded:   humanReadableSize(uploaded),
			Downloaded: humanReadableSize(downloaded),
			Removed:    humanReadableSize(removed),
			Prunable:   humanReadableSize(prunable),

			DownloadSpeedMBPS: downloadedMBPS,
			UploadSpeedMBPS:   uploadedMBPS,

			DatasetComplete: complete,
		}
		if err != nil {
			res.Err = &resultErr{err}
		}
	}(time.Now())

	// update redundancy
	err = withSaneTimeout(func(ctx context.Context) error {
		us, err := bc.UploadSettings(ctx)
		if err != nil {
			return err
		}
		rs = us.Redundancy
		return nil
	}, nil)
	if err != nil {
		err = fmt.Errorf("failed to refresh redundancy; %w", err)
		return
	}

	// ensure our dataset matches requested size
	start := time.Now()
	uploaded, _, err = ensureDataset(cfg.DatasetSize)
	if err != nil {
		err = fmt.Errorf("failed to ensure dataset; %w", err)
		return
	}
	uploadedMBPS = mbps(downloaded, time.Since(start).Milliseconds())
	complete = true

	size := int64(cfg.IntegrityCheckDownloadPct * float64(cfg.DatasetSize))
	logger.Infof("checking integrity of %d%% of our dataset (%v)", int(cfg.IntegrityCheckDownloadPct*100), humanReadableSize(size))

	// check integrity of a portion of the dataset
	start = time.Now()
	downloaded, err = checkIntegrity(size)
	if err != nil {
		err = fmt.Errorf("failed to check integrity of the dataset; %w", err)
		return
	}
	downloadedMBPS = mbps(downloaded, time.Since(start).Milliseconds())

	// delete data
	size = int64(cfg.IntegrityCheckDeletePct * float64(cfg.DatasetSize))
	logger.Infof("deleting %d%% of our dataset (%v)", int(cfg.IntegrityCheckDeletePct*100), humanReadableSize(size))
	removed, err = pruneDataset(size)
	if err != nil {
		err = fmt.Errorf("failed to prune the dataset, removed %d; %w", removed, err)
		return
	}

	// update redundancy
	if err = withSaneTimeout(func(ctx context.Context) error {
		res, err := bc.PrunableData(ctx)
		if err != nil {
			return err
		}
		prunable = int64(res.TotalPrunable)
		return nil
	}, nil); err != nil {
		err = fmt.Errorf("failed to fetch prunable data; %w", err)
		return
	}

	return
}

func registerAlert(res result) error {
	// set severity level
	severity := alerts.SeverityInfo
	if err := res.Error(); errors.Is(err, errIntegrity) {
		severity = alerts.SeverityCritical
	}

	// set data source
	data := make(map[string]any)
	data["source"] = "renterd-integrity"
	data["result"] = res

	// set message
	msg := "integrity check completed successfully"
	if err := res.Error(); err != nil {
		msg = fmt.Sprintf("integrity check failed, err: %v", err)
	}

	// create alert
	alert := alerts.Alert{
		ID:        randomID(),
		Severity:  severity,
		Message:   msg,
		Data:      data,
		Timestamp: time.Now(),
	}

	logger.Debugf("registered alert: %v", alert.Message)
	return withSaneTimeout(func(ctx context.Context) error {
		return bc.RegisterAlert(ctx, alert)
	}, nil)
}
