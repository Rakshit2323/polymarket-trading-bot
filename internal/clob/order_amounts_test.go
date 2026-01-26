package clob

import (
	"math/big"
	"testing"
)

func TestComputeMarketOrderAmountsFromPrice_Buy_RoundsToAllowedDecimals(t *testing.T) {
	priceScale := big.NewInt(100) // tick=0.01
	priceTicks := big.NewInt(37)  // $0.37
	makerIn := big.NewInt(1234567)

	makerOut, takerOut, err := computeMarketOrderAmountsFromPrice(SideBuy, makerIn, priceTicks, priceScale)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if makerOut.Sign() <= 0 || takerOut.Sign() <= 0 {
		t.Fatalf("expected positive outputs, got maker=%s taker=%s", makerOut.String(), takerOut.String())
	}

	// BUY: maker=collateral rounded to 2 decimals => multiple of 10^(6-2)=10000.
	if new(big.Int).Mod(makerOut, big.NewInt(10_000)).Sign() != 0 {
		t.Fatalf("maker not 2dp: maker=%s", makerOut.String())
	}
	// BUY: taker=shares rounded to 4 decimals => multiple of 10^(6-4)=100.
	if new(big.Int).Mod(takerOut, big.NewInt(100)).Sign() != 0 {
		t.Fatalf("taker not 4dp: taker=%s", takerOut.String())
	}
}

func TestComputeMarketOrderAmountsFromPrice_Buy_RoundsMakerToNearestCent(t *testing.T) {
	// Regression for min-size failures when a $1.00-ish onchain amount got floored to $0.99.
	priceScale := big.NewInt(100) // tick=0.01
	priceTicks := big.NewInt(10)  // $0.10
	makerIn := big.NewInt(999_999)

	makerOut, takerOut, err := computeMarketOrderAmountsFromPrice(SideBuy, makerIn, priceTicks, priceScale)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}

	if makerOut.Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatalf("maker rounding mismatch: got %s want %s", makerOut.String(), "1000000")
	}
	if takerOut.Cmp(big.NewInt(10_000_000)) != 0 {
		t.Fatalf("taker mismatch: got %s want %s", takerOut.String(), "10000000")
	}
}

func TestComputeMarketOrderAmountsFromPrice_Sell_RoundsToAllowedDecimals(t *testing.T) {
	priceScale := big.NewInt(100) // tick=0.01
	priceTicks := big.NewInt(37)  // $0.37
	makerIn := big.NewInt(9_876_543)

	makerOut, takerOut, err := computeMarketOrderAmountsFromPrice(SideSell, makerIn, priceTicks, priceScale)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if makerOut.Sign() <= 0 || takerOut.Sign() <= 0 {
		t.Fatalf("expected positive outputs, got maker=%s taker=%s", makerOut.String(), takerOut.String())
	}

	// SELL: maker=shares rounded to 2 decimals => multiple of 10^(6-2)=10000.
	if new(big.Int).Mod(makerOut, big.NewInt(10_000)).Sign() != 0 {
		t.Fatalf("maker not 2dp: maker=%s", makerOut.String())
	}
	// SELL: taker=collateral rounded to 4 decimals => multiple of 10^(6-4)=100.
	if new(big.Int).Mod(takerOut, big.NewInt(100)).Sign() != 0 {
		t.Fatalf("taker not 4dp: taker=%s", takerOut.String())
	}
}

func TestComputeMarketOrderAmountsFromPrice_DoesNotDependOnTickPrecision(t *testing.T) {
	// tick=0.001 (3 price decimals) should not change max amount precision.
	priceScale := big.NewInt(1_000)
	priceTicks := big.NewInt(512) // $0.512
	makerIn := big.NewInt(1_234_567)

	makerOut, takerOut, err := computeMarketOrderAmountsFromPrice(SideBuy, makerIn, priceTicks, priceScale)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if new(big.Int).Mod(makerOut, big.NewInt(10_000)).Sign() != 0 {
		t.Fatalf("maker not 2dp: maker=%s", makerOut.String())
	}
	if new(big.Int).Mod(takerOut, big.NewInt(100)).Sign() != 0 {
		t.Fatalf("taker not 4dp: taker=%s", takerOut.String())
	}
}
