package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

type (
	state struct {
		Ok      bool     `json:"ok"`
		Results []result `json:"results"`
	}

	result struct {
		StartedAt time.Time `json:"startedAt"`
		EndedAt   time.Time `json:"endedAt"`

		Downloaded string `json:"downloaded,omitempty"`
		Uploaded   string `json:"uploaded,omitempty"`
		Removed    string `json:"removed,omitempty"`
		Pruned     string `json:"pruned,omitempty"`

		DownloadSpeedMBPS float64       `json:"downloadSpeedMBPS,omitempty"`
		UploadSpeedMBPS   float64       `json:"uploadSpeedMBPS,omitempty"`
		PruneElapsedTime  time.Duration `json:"pruneElapsedTime,omitempty"`

		DatasetComplete bool       `json:"datasetComplete"`
		Err             *resultErr `json:"error,omitempty"`
	}

	resultErr struct {
		Err error
	}
)

func (s *state) timeSinceLastIntegrityCheck() time.Duration {
	if len(s.Results) == 0 || !s.Results[0].DatasetComplete {
		return math.MaxInt64
	}
	return time.Since(s.Results[0].StartedAt)
}

func saveState(s *state, path string) error {
	// open the file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to open state file at '%s', err: %v", path, err)
	}
	defer f.Close()

	// trim the results
	if len(s.Results) > 30 {
		s.Results = s.Results[:30]
	}

	// update overall OK status
	s.Ok = true
	for _, res := range s.Results {
		s.Ok = s.Ok && res.Err.Err == nil
	}

	// encode the state
	enc := json.NewEncoder(f)
	if err := enc.Encode(s); err != nil {
		return err
	}
	return nil
}

func loadState(path string) (s *state, _ error) {
	// check whether the state file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return &state{}, nil
	}

	// open the file
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file at '%s', err: %v", path, err)
	}
	defer f.Close()

	// decode the state
	dec := json.NewDecoder(f)
	if err := dec.Decode(&s); err != nil && errors.Is(err, io.EOF) {
		return &state{}, nil
	} else if err != nil {
		return nil, err
	}
	return
}

func (r result) Error() error {
	if r.Err != nil {
		return r.Err.Err
	}
	return nil
}

func (e *resultErr) MarshalJSON() ([]byte, error) {
	var errStr string
	if e.Err != nil {
		errStr = e.Err.Error()
	}
	return json.Marshal(errStr)
}

func (e *resultErr) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	if str != "" {
		e.Err = errors.New(str)
	}
	return nil
}
