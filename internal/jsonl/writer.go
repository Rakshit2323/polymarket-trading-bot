package jsonl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Writer appends newline-delimited JSON (JSONL) records to a file.
//
// It is safe for concurrent use.
type Writer struct {
	mu   sync.Mutex
	path string
	file *os.File
	w    *bufio.Writer
}

// New returns a JSONL writer that appends to path. If path is empty/blank, it
// returns nil.
func New(path string) *Writer {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	return &Writer{path: path}
}

func (w *Writer) ensureOpenLocked() error {
	if w.file != nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	w.file = f
	w.w = bufio.NewWriterSize(f, 256*1024)
	return nil
}

// Write appends v as a single JSON object followed by '\n'.
// It flushes the buffered writer to make the record visible to tailers.
func (w *Writer) Write(v any) error {
	if w == nil {
		return nil
	}
	if v == nil {
		return fmt.Errorf("jsonl: nil record")
	}

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureOpenLocked(); err != nil {
		return err
	}

	if _, err := w.w.Write(b); err != nil {
		return err
	}
	if err := w.w.WriteByte('\n'); err != nil {
		return err
	}
	return w.w.Flush()
}

// Close flushes any buffered data and closes the underlying file.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	if w.w != nil {
		if err := w.w.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.w = nil
	w.file = nil

	if firstErr != nil && errors.Is(firstErr, os.ErrClosed) {
		return nil
	}
	return firstErr
}
