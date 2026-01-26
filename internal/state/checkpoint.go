package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Checkpoint struct {
	ChainID int64 `json:"chain_id"`

	// Legacy single-leader field (kept for backward compatibility).
	LeaderAddress string `json:"leader_address"`
	// Preferred multi-leader field. If set, it must match the runtime leader set
	// exactly for a checkpoint to be considered compatible.
	LeaderAddresses []string `json:"leader_addresses,omitempty"`

	ExchangeAddress string `json:"exchange_address"`

	LastProcessedBlock    uint64 `json:"last_processed_block"`
	LastProcessedLogIndex uint   `json:"last_processed_log_index"`
}

func LoadCheckpoint(path string) (Checkpoint, bool, error) {
	if path == "" {
		return Checkpoint{}, false, nil
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, err
	}

	var ckpt Checkpoint
	if err := json.Unmarshal(b, &ckpt); err != nil {
		return Checkpoint{}, false, fmt.Errorf("parse checkpoint %s: %w", path, err)
	}
	return ckpt, true, nil
}

func SaveCheckpoint(path string, ckpt Checkpoint) error {
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(ckpt, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
