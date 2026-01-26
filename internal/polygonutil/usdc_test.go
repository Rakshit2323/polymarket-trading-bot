package polygonutil

import (
	"math"
	"math/big"
	"testing"
)

func TestUint64FromUint256Saturating(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		if got := uint64FromUint256Saturating(nil); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})

	t.Run("zero", func(t *testing.T) {
		t.Parallel()
		if got := uint64FromUint256Saturating(big.NewInt(0)); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})

	t.Run("negative", func(t *testing.T) {
		t.Parallel()
		if got := uint64FromUint256Saturating(big.NewInt(-1)); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})

	t.Run("fits_uint64", func(t *testing.T) {
		t.Parallel()
		want := uint64(123_456_789)
		if got := uint64FromUint256Saturating(new(big.Int).SetUint64(want)); got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	})

	t.Run("overflows_uint64", func(t *testing.T) {
		t.Parallel()
		over := new(big.Int).Add(new(big.Int).SetUint64(math.MaxUint64), big.NewInt(1))
		if got := uint64FromUint256Saturating(over); got != math.MaxUint64 {
			t.Fatalf("got %d, want %d", got, uint64(math.MaxUint64))
		}
	})
}

