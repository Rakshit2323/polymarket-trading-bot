package arbbot

import (
	"testing"

	"poly-gocopy/internal/arbitrage"
)

func TestFillBySpend(t *testing.T) {
	levels := []arbitrage.Level{
		{PriceMicros: 520_000, SharesMicros: 2 * microsScale},
		{PriceMicros: 530_000, SharesMicros: 3 * microsScale},
		{PriceMicros: 550_000, SharesMicros: 3 * microsScale},
	}
	budget := uint64(4_280_000)
	fill := fillBySpend(levels, budget)
	if fill.sharesMicros != 8*microsScale {
		t.Fatalf("shares=%d want %d", fill.sharesMicros, 8*microsScale)
	}
	if fill.costMicros != budget {
		t.Fatalf("cost=%d want %d", fill.costMicros, budget)
	}
	if fill.avgPriceMicros != 535_000 {
		t.Fatalf("avg=%d want %d", fill.avgPriceMicros, 535_000)
	}
}

func TestMaxSharesByAvg_AllowsOffset(t *testing.T) {
	levels := []arbitrage.Level{
		{PriceMicros: 200_000, SharesMicros: 1 * microsScale},
		{PriceMicros: 800_000, SharesMicros: 1 * microsScale},
	}
	fill := maxSharesByAvg(levels, 500_000)
	if fill.sharesMicros != 2*microsScale {
		t.Fatalf("shares=%d want %d", fill.sharesMicros, 2*microsScale)
	}
	if fill.avgPriceMicros != 500_000 {
		t.Fatalf("avg=%d want %d", fill.avgPriceMicros, 500_000)
	}
}

func TestParseEntryPctMicros(t *testing.T) {
	cases := []struct {
		raw  string
		want uint64
	}{
		{raw: "10%", want: 100_000},
		{raw: "0.25", want: 250_000},
		{raw: "5", want: 50_000},
	}
	for _, tc := range cases {
		got, err := parseEntryPctMicros(tc.raw)
		if err != nil {
			t.Fatalf("parse %q err=%v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("parse %q got %d want %d", tc.raw, got, tc.want)
		}
	}
}
