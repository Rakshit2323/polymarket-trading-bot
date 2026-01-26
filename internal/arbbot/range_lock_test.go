package arbbot

import (
	"context"
	"testing"
	"time"

	"poly-gocopy/internal/arbitrage"
)

func TestRangeEntryDoesNotRepeatBuysAfterRoleFlip(t *testing.T) {
	st := botState{
		a: sideState{tokenID: "A", capMicros: 10 * microsScale},
		b: sideState{tokenID: "B", capMicros: 10 * microsScale},
	}
	parsed := args{
		enableTrading:         false,
		winRangeMinMicros:     520_000,
		winRangeMaxMicros:     850_000,
		loseRangeMinMicros:    150_000,
		loseRangeMaxMicros:    460_000,
		rangeEntryTotalMicros: 3 * microsScale,
	}

	bestA := arbitrage.BestAsk{PriceMicros: 600_000, SharesMicros: 2 * microsScale}
	bestB := arbitrage.BestAsk{PriceMicros: 400_000, SharesMicros: 2 * microsScale}
	stepRangeBuys(
		context.Background(),
		nil,
		parsed,
		&st,
		bestA,
		bestB,
		nil,
		nil,
		func() int64 { return 0 },
		nil,
		time.Unix(0, 0),
		"",
		time.Time{},
	)

	spentA := st.a.spentMicros
	spentB := st.b.spentMicros

	bestA = arbitrage.BestAsk{PriceMicros: 300_000, SharesMicros: 2 * microsScale}
	bestB = arbitrage.BestAsk{PriceMicros: 700_000, SharesMicros: 2 * microsScale}
	stepRangeBuys(
		context.Background(),
		nil,
		parsed,
		&st,
		bestA,
		bestB,
		nil,
		nil,
		func() int64 { return 0 },
		nil,
		time.Unix(0, 0),
		"",
		time.Time{},
	)

	if st.a.spentMicros != spentA {
		t.Fatalf("token A spent changed after role flip: got %d want %d", st.a.spentMicros, spentA)
	}
	if st.b.spentMicros != spentB {
		t.Fatalf("token B spent changed after role flip: got %d want %d", st.b.spentMicros, spentB)
	}
}

func TestRangeEntryDoesNotBuySecondLegOnWinnerFlip(t *testing.T) {
	st := botState{
		a: sideState{tokenID: "A", capMicros: 10 * microsScale},
		b: sideState{tokenID: "B", capMicros: 10 * microsScale},
	}
	parsed := args{
		enableTrading:         false,
		winRangeMinMicros:     520_000,
		winRangeMaxMicros:     850_000,
		loseRangeMinMicros:    150_000,
		loseRangeMaxMicros:    460_000,
		rangeEntryTotalMicros: 3 * microsScale,
	}

	bestA := arbitrage.BestAsk{PriceMicros: 600_000, SharesMicros: 2 * microsScale}
	bestB := arbitrage.BestAsk{PriceMicros: 480_000, SharesMicros: 2 * microsScale}
	stepRangeBuys(
		context.Background(),
		nil,
		parsed,
		&st,
		bestA,
		bestB,
		nil,
		nil,
		func() int64 { return 0 },
		nil,
		time.Unix(0, 0),
		"",
		time.Time{},
	)

	if !st.rangeWinBought || st.rangeLoseBought {
		t.Fatalf("expected win leg only after first entry (win=%v lose=%v)", st.rangeWinBought, st.rangeLoseBought)
	}
	if st.a.spentMicros == 0 || st.b.spentMicros != 0 {
		t.Fatalf("unexpected spend after first entry: a=%d b=%d", st.a.spentMicros, st.b.spentMicros)
	}
	spentA := st.a.spentMicros
	spentB := st.b.spentMicros

	bestA = arbitrage.BestAsk{PriceMicros: 300_000, SharesMicros: 2 * microsScale}
	bestB = arbitrage.BestAsk{PriceMicros: 700_000, SharesMicros: 2 * microsScale}
	stepRangeBuys(
		context.Background(),
		nil,
		parsed,
		&st,
		bestA,
		bestB,
		nil,
		nil,
		func() int64 { return 0 },
		nil,
		time.Unix(0, 0),
		"",
		time.Time{},
	)

	if st.a.spentMicros != spentA {
		t.Fatalf("token A spent changed after winner flip: got %d want %d", st.a.spentMicros, spentA)
	}
	if st.b.spentMicros != spentB {
		t.Fatalf("token B spent changed after winner flip: got %d want %d", st.b.spentMicros, spentB)
	}
	if st.rangeLoseBought {
		t.Fatalf("unexpected second leg after winner flip")
	}
}
