package arbitrage

import "sort"

// Level represents an order book ask level (or any price+size bucket).
// Both price and size are in micro units (1e6 scale).
type Level struct {
	PriceMicros  uint64
	SharesMicros uint64
}

// NormalizeLevels returns levels sorted ascending by PriceMicros, with same-price
// levels merged and zero-size levels removed.
func NormalizeLevels(levels []Level) []Level {
	out := make([]Level, 0, len(levels))
	for _, l := range levels {
		if l.PriceMicros == 0 || l.SharesMicros == 0 {
			continue
		}
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PriceMicros == out[j].PriceMicros {
			return out[i].SharesMicros < out[j].SharesMicros
		}
		return out[i].PriceMicros < out[j].PriceMicros
	})
	merged := out[:0]
	for _, l := range out {
		if len(merged) == 0 || merged[len(merged)-1].PriceMicros != l.PriceMicros {
			merged = append(merged, l)
			continue
		}
		merged[len(merged)-1].SharesMicros += l.SharesMicros
	}
	return merged
}
