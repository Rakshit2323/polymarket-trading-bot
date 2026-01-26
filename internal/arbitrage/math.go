package arbitrage

import (
	"math"
	"math/big"
	"math/bits"
)

func mulDivU64(a, b, div uint64) uint64 {
	if div == 0 {
		panic("mulDivU64: div=0")
	}

	hi, lo := bits.Mul64(a, b)
	if hi == 0 {
		return lo / div
	}

	// Fallback for overflow cases: exact 128-bit division via big.Int.
	var x big.Int
	x.SetUint64(hi)
	x.Lsh(&x, 64)

	var y big.Int
	y.SetUint64(lo)
	x.Add(&x, &y)

	var d big.Int
	d.SetUint64(div)
	x.Div(&x, &d)

	if x.IsUint64() {
		return x.Uint64()
	}
	return math.MaxUint64
}

func costMicrosForShares(sharesMicros, priceMicros uint64) uint64 {
	return mulDivU64(sharesMicros, priceMicros, microsScale)
}

func maxSharesFromCapMicros(remainingCapMicros, priceMicros uint64) uint64 {
	if priceMicros == 0 {
		return 0
	}
	return mulDivU64(remainingCapMicros, microsScale, priceMicros)
}

// CostMicrosForShares returns the collateral (in micros) required to buy
// sharesMicros shares at priceMicros.
func CostMicrosForShares(sharesMicros, priceMicros uint64) uint64 {
	return costMicrosForShares(sharesMicros, priceMicros)
}

// MaxSharesFromCapMicros returns the maximum shares (in micros) that can be
// bought at priceMicros using remainingCapMicros collateral (in micros).
func MaxSharesFromCapMicros(remainingCapMicros, priceMicros uint64) uint64 {
	return maxSharesFromCapMicros(remainingCapMicros, priceMicros)
}
