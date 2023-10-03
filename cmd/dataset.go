package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/renterd/api"
	"lukechampine.com/blake3"
	"lukechampine.com/frand"
)

const (
	blake3HashDigestSize = 16

	dataExtension = ".data"

	defaultChunkSize = int64(1 << 26) // 64 MiB

	defaultPruneTimeout = 30 * time.Minute
)

func ensureDataset(want int64) (added, removed int64, _ error) {
	// calculate size of the current set
	got, err := calculateDatasetSize()
	if err != nil {
		return 0, 0, err
	}

	// remove excess data if necessary
	if got > want {
		logger.Infof("ensuring data set size matches %s - current size %s removing %s", humanReadableSize(want), humanReadableSize(got), humanReadableSize(got-want))
		toRemove, err := calculateRandomBatch(got - want)
		if err != nil {
			return 0, 0, err
		}
		for _, entry := range toRemove {
			if err = withSaneTimeout(func(ctx context.Context) error {
				return bc.DeleteObject(ctx, api.DefaultBucketName, entry.Name, false)
			}, nil); err != nil {
				return
			} else {
				removed += entry.Size
			}
		}
		got, err = calculateDatasetSize()
		if err != nil {
			return 0, removed, err
		}
	}

	// add missing data if necessary
	if got < want {
		logger.Infof("ensuring data set size matches %s - adding %s", humanReadableSize(want), humanReadableSize(want-got))

		// fetch the redundancy settings
		var rs api.RedundancySettings
		if err = withSaneTimeout(func(ctx context.Context) (err error) {
			rs, err = bc.RedundancySettings(ctx)
			return
		}, nil); err != nil {
			return 0, removed, err
		}

		// take into account redundancy in the return value
		defer func() { added = int64(float64(added) * rs.Redundancy()) }()

		// find out how much data we are missing
		missing := want - got
		if missing < cfg.MinFilesize {
			missing = cfg.MinFilesize
		}

		for {
			// calculate a random file size between our min and max
			max := int64(math.Min(float64(missing), float64(cfg.MaxFilesize)))
			min := int64(cfg.MinFilesize)

			// round to the nearest chunk size
			size := max
			if max > min {
				size = int64(frand.Intn(int(max-min))) + min
			}

			// upload the file
			_, err = uploadFile(size)
			if err != nil {
				return added, removed, err
			}
			added += size

			// break if necessary
			missing -= size
			if missing <= 0 {
				break
			}
		}
	}

	return
}

func pruneDataset(size int64) (removed, pruned int64, elapsed time.Duration, err error) {
	entries, err := calculateRandomBatch(size)
	if err != nil {
		return 0, 0, 0, err
	}

	// set elapsed
	defer func(start time.Time) {
		elapsed = time.Since(start)
	}(time.Now())

	// remove the data
	for _, entry := range entries {
		if err = withSaneTimeout(func(ctx context.Context) (err error) {
			return bc.DeleteObject(ctx, api.DefaultBucketName, entry.Name, false)
		}, nil); err != nil {
			return
		}
		removed += entry.Size
	}

	// prune the contracts
	var prunable api.ContractsPrunableDataResponse
	if err = withSaneTimeout(func(ctx context.Context) (err error) {
		prunable, err = bc.PrunableData(ctx)
		return
	}, nil); err != nil {
		return
	}

	for _, contract := range prunable.Contracts {
		start := time.Now()
		logger.Debugf("pruning contract %+v", contract)
		if err = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*defaultPruneTimeout)
			defer cancel()

			ppruned, remaining, err := wc.RHPPruneContract(ctx, contract.ID, defaultPruneTimeout)
			if err != nil {
				return err
			}
			pruned += int64(ppruned)

			logger.Debugf("pruned %v bytes from contract %v, %v bytes remaining", ppruned, contract.ID, remaining)
			return nil
		}(); err != nil {
			logger.Debugf("pruning contract %v failed after %v, err %v", contract.ID, time.Since(start), err)
			return
		}
	}
	return
}

func calculateDatasetSize() (size int64, _ error) {
	entries, err := fetchEntries()
	if err != nil {
		return 0, err
	}

	for _, entry := range entries {
		size += entry.Size
	}
	return
}

func calculateRandomBatch(size int64) (batch []api.ObjectMetadata, _ error) {
	entries, err := fetchEntries()
	if err != nil {
		return nil, err
	}

	frand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})
	for _, entry := range entries {
		batch = append(batch, entry)
		size -= entry.Size
		if size <= 0 {
			break
		}
	}
	return
}

// TODO: this fetches all entries, which is not ideal
func fetchEntries() (entries []api.ObjectMetadata, err error) {
	err = withSaneTimeout(func(ctx context.Context) (err error) {
		entries, err = bc.SearchObjects(ctx, api.DefaultBucketName, cfg.WorkDir, 0, -1)
		return
	}, nil)
	return
}

func createRandomFile(size int64) (_ string, err error) {
	tmp := cfg.buildTmpFilepath()

	var f *os.File
	f, err = os.Create(tmp)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
			return
		}

		_ = f.Sync()
		_ = f.Close()
	}()

	h := blake3.New(blake3HashDigestSize, nil)

	chunkSize := defaultChunkSize
	remainging := size
	if remainging < chunkSize {
		chunkSize = remainging
	}

	chunk := make([]byte, chunkSize)
	for remainging > 0 {
		_, err = frand.Read(chunk)
		if err != nil {
			return
		}

		_, err = h.Write(chunk)
		if err != nil {
			return
		}

		_, err = f.Write(chunk)
		if err != nil {
			return
		}

		remainging -= int64(len(chunk))
	}

	dst := cfg.buildHashFilepath(h)
	err = os.Rename(tmp, dst)
	if err != nil {
		return
	}

	return dst, nil
}

func uploadFile(size int64) (path string, err error) {
	totalSize := int64(float64(size) * rs.Redundancy())
	logger.Debugf("uploading %v", humanReadableSize(size))
	start := time.Now()
	defer func() {
		if err == nil {
			elapsed := time.Since(start)
			logger.Debugf("uploaded file to %v in %v (%v mbps)", path, elapsed, mbps(totalSize, elapsed.Milliseconds()))
		}
	}()

	// create random file
	path, err = createRandomFile(size)
	if err != nil {
		return "", err
	}

	// defer the removal
	defer func() {
		if err := os.Remove(path); err != nil {
			logger.Errorf("failed to remove file at path '%v', err: %v", path, err)
		}
	}()

	// open the file
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return
	}

	// defer a close
	defer f.Close()

	// upload the file
	err = withSaneTimeout(func(ctx context.Context) error {
		_, err := wc.UploadObject(ctx, f, path)
		return err
	}, &totalSize)
	return
}

func downloadFile(path string, size int64) (_ string, err error) {
	logger.Debugf("downloading %v", humanReadableSize(size))
	start := time.Now()
	defer func() {
		if err == nil {
			elapsed := time.Since(start)
			logger.Debugf("downloaded file %v in %v (%v mbps)", path, elapsed, mbps(int64(float64(size)*rs.Redundancy()), elapsed.Milliseconds()))
		}
	}()

	// create a tmp file to download to
	tmpfile := filepath.Join(os.TempDir(), filepath.Base(path))
	f, err := os.Create(tmpfile)
	if err != nil {
		return "", err
	}

	// cleanup the file when we're done
	defer func() {
		if err := os.Remove(tmpfile); err != nil {
			logger.Errorf("failed to remove tmp download file, err %v", err)
		}
	}()

	// download the file
	err = withSaneTimeout(func(ctx context.Context) error {
		return wc.DownloadObject(ctx, f, path)
	}, &size)
	if err != nil {
		return "", err
	}

	// seek to the beginning
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	// hash the file
	h := blake3.New(blake3HashDigestSize, nil)
	for {
		chunk := make([]byte, defaultChunkSize)
		n, err := f.Read(chunk)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return "", err
		}

		chunk = chunk[:n]
		_, err = h.Write(chunk)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func checkIntegrity(size int64) (downloaded int64, err error) {
	logger.Debugf("checking integrity of %v files", humanReadableSize(size))
	toDownload, err := calculateRandomBatch(size)
	if err != nil {
		return 0, err
	}

	for _, entry := range toDownload {
		var hash string
		hash, err = downloadFile(entry.Name, entry.Size)
		if err != nil {
			return
		}

		downloaded += entry.Size
		expected := strings.TrimSuffix(filepath.Base(entry.Name), dataExtension)
		if hash != expected {
			err = fmt.Errorf("hash mismatch for file '%v', expected '%v', got '%v'; %w", entry.Name, expected, hash, errIntegrity)
			logger.Error(err)
			return
		}
	}

	return
}
