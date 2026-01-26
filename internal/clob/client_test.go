package clob

import (
	"encoding/json"
	"testing"
)

func TestTickSizeResp_UnmarshalNumber(t *testing.T) {
	var resp tickSizeResp
	if err := json.Unmarshal([]byte(`{"minimum_tick_size":0.01}`), &resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := string(resp.MinimumTickSize), "0.01"; got != want {
		t.Fatalf("minimum_tick_size mismatch: got %q want %q", got, want)
	}
}

func TestTickSizeResp_UnmarshalStringAndCanonicalize(t *testing.T) {
	var resp tickSizeResp
	if err := json.Unmarshal([]byte(`{"minimum_tick_size":"0.0100"}`), &resp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := string(resp.MinimumTickSize), "0.01"; got != want {
		t.Fatalf("minimum_tick_size mismatch: got %q want %q", got, want)
	}
}
