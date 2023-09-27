package main

import (
	"encoding/json"
	"fmt"
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

		DatasetComplete bool   `json:"datasetComplete"`
		Err             string `json:"error,omitempty"`
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
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	return
}
