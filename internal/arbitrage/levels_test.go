package arbitrage

import "testing"

func TestNormalizeLevels_MergesSamePrice(t *testing.T) {
	in := []Level{
		{PriceMicros: 500_000, SharesMicros: 2 * microsScale},
		{PriceMicros: 500_000, SharesMicros: 3 * microsScale},
		{PriceMicros: 490_000, SharesMicros: 1 * microsScale},
	}
	out := NormalizeLevels(in)
	if len(out) != 2 {
		t.Fatalf("len(out)=%d want %d", len(out), 2)
	}
	if out[0].PriceMicros != 490_000 {
		t.Fatalf("out[0].price=%d want %d", out[0].PriceMicros, 490_000)
	}
	if out[1].PriceMicros != 500_000 || out[1].SharesMicros != 5*microsScale {
		t.Fatalf("out[1]=%+v want price=500000 shares=%d", out[1], 5*microsScale)
	}
}
