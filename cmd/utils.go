package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func withSaneTimeout(fn func(ctx context.Context) error, size *int64) error {
	timeout := time.Minute // min

	// if we have a size, calculate the timeout based on the size, we use a
	// pessimistic speed of 1mbps, which should be more than fine for both
	// uploads and downloads
	if size != nil {
		fsize := float64(*size)
		fsize *= 0.000008

		dur := time.Second * time.Duration(fsize)
		if dur > timeout {
			timeout = dur
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return fn(ctx)
}

func humanReadableSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%d bytes (%.1f %ciB)", b, float64(b)/float64(div), "KMGTPE"[exp])
}

func mbps(b, ms int64) float64 {
	bpms := float64(b) / float64(ms)
	return math.Round(bpms*0.008*100) / 100
}

func randomString() string {
	b := make([]byte, 16)
	_, _ = frand.Read(b)
	return hex.EncodeToString(b)
}

func randomID() types.Hash256 {
	var id types.Hash256
	_, _ = frand.Read(id[:])
	return id
}
