package arbbot

import "testing"

func TestPriceChangeKeyMicros(t *testing.T) {
	tests := []struct {
		in   uint64
		want uint64
	}{
		{in: 0, want: 0},
		{in: 9_999, want: 0},
		{in: 10_000, want: 10_000},
		{in: 10_001, want: 10_000},
		{in: 550_000, want: 550_000},
		{in: 550_001, want: 550_000},
		{in: 999_999, want: 990_000},
		{in: 1_000_000, want: 1_000_000},
	}

	for _, tt := range tests {
		if got := priceChangeKeyMicros(tt.in); got != tt.want {
			t.Fatalf("priceChangeKeyMicros(%d)=%d want %d", tt.in, got, tt.want)
		}
	}
}
