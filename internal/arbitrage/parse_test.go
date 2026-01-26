package arbitrage

import "testing"

func TestParseMicros(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"1", 1_000_000},
		{"1.0", 1_000_000},
		{"0.55", 550_000},
		{".5", 500_000},
		{"1.000001", 1_000_001},
		{"1.0000019", 1_000_001}, // truncate beyond 6dp
		{"  0.0100 ", 10_000},
		{"+2.5", 2_500_000},
	}

	for _, tt := range tests {
		got, err := ParseMicros(tt.in)
		if err != nil {
			t.Fatalf("ParseMicros(%q) unexpected error: %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("ParseMicros(%q)=%d want %d", tt.in, got, tt.want)
		}
	}
}

func TestParseMicros_Invalid(t *testing.T) {
	bad := []string{
		"",
		"   ",
		"-1",
		"1.2.3",
		"abc",
		"1-2",
		".",
		"+",
	}
	for _, in := range bad {
		if _, err := ParseMicros(in); err == nil {
			t.Fatalf("ParseMicros(%q) expected error", in)
		}
	}
}
