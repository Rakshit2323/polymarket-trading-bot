package arbbot

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"poly-gocopy/internal/arbitrage"
	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/jsonl"
)

const (
	equalStatusMinInterval = 10 * time.Second
	equalRetryMinInterval  = 2 * time.Second
)

type equalState struct {
	targetSharesMicros uint64
	targetAvgMicros    uint64
	targetSet          bool
	baseDone           bool

	assignedSpentMicros uint64

	lastAttemptAt time.Time
	status        statusTracker
}

func RunEqual() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	parsed, err := parseEqualArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	runStrategy(parsed, "equal", stepEqualBuys, logEqualConfig, nil)
}

func logEqualConfig(parsed args) {
	totalBudget := parsed.equalAssignedBudgetMicros * 2
	targetAvg := parsed.equalTargetAvgMicros
	if targetAvg == 0 && parsed.equalSumMaxMicros > 0 {
		targetAvg = parsed.equalSumMaxMicros / 2
	}
	log.Printf(
		"Equal entry: per_side=%s total=%s start_delay=%s",
		formatMicros(parsed.equalAssignedBudgetMicros),
		formatMicros(totalBudget),
		parsed.equalStartDelay,
	)
	log.Printf(
		"Equal targets: target_avg=%s sum_cap<=%s abort>=%s",
		formatMicros(targetAvg),
		formatMicros(parsed.equalSumMaxMicros),
		formatMicros(parsed.equalSumAbortMicros),
	)
	log.Printf("Equal drift: zero within last 3 minutes of the window")
	log.Printf("Equal buys: order_type=FAK (fixed)")
}

func parseEqualArgs() (args, error) {
	parsed, err := parseArgs()
	if err != nil {
		return args{}, err
	}

	parsed.rangeStopLossAfter = 0
	parsed.rangeStopLossBudgetMicros = 0
	parsed.rangeStopLossMaxPriceMicros = 0
	parsed.rangeStopLossMarket = false

	equalStartDelay := 2 * time.Minute
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_EQUAL_START_DELAY")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_EQUAL_START_DELAY %q: %w", env, err)
		}
		equalStartDelay = v
	} else if env := strings.TrimSpace(os.Getenv("ARBITRAGE_START_DELAY")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_START_DELAY %q: %w", env, err)
		}
		equalStartDelay = v
	}
	if equalStartDelay < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_EQUAL_START_DELAY must be >= 0")
	}
	parsed.equalStartDelay = equalStartDelay

	assignedRaw := strings.TrimSpace(os.Getenv("ARBITRAGE_EQUAL_ASSIGNED_USD"))
	if assignedRaw == "" {
		return args{}, fmt.Errorf("ARBITRAGE_EQUAL_ASSIGNED_USD is required")
	}
	assignedBudgetMicros, err := arbitrage.ParseMicros(assignedRaw)
	if err != nil || assignedBudgetMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_EQUAL_ASSIGNED_USD %q", assignedRaw)
	}
	if assignedBudgetMicros < minBuyMicros {
		return args{}, fmt.Errorf("ARBITRAGE_EQUAL_ASSIGNED_USD must be >= %s", formatMicros(minBuyMicros))
	}
	parsed.equalAssignedBudgetMicros = assignedBudgetMicros

	sumMaxRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_EQUAL_SUM_MAX"), "0.97"))
	sumAbortRaw := strings.TrimSpace(os.Getenv("ARBITRAGE_EQUAL_SUM_ABORT"))
	targetAvgRaw := strings.TrimSpace(os.Getenv("ARBITRAGE_EQUAL_TARGET_AVG"))

	sumMaxMicros, err := arbitrage.ParseMicros(sumMaxRaw)
	if err != nil || sumMaxMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_EQUAL_SUM_MAX %q", sumMaxRaw)
	}
	parsed.equalSumMaxMicros = sumMaxMicros

	sumAbortMicros := sumMaxMicros
	if sumAbortRaw != "" {
		v, err := arbitrage.ParseMicros(sumAbortRaw)
		if err != nil || v == 0 {
			return args{}, fmt.Errorf("invalid ARBITRAGE_EQUAL_SUM_ABORT %q", sumAbortRaw)
		}
		sumAbortMicros = v
	}

	targetAvgMicros := uint64(0)
	if targetAvgRaw != "" {
		v, err := arbitrage.ParseMicros(targetAvgRaw)
		if err != nil || v == 0 {
			return args{}, fmt.Errorf("invalid ARBITRAGE_EQUAL_TARGET_AVG %q", targetAvgRaw)
		}
		targetAvgMicros = v
	} else {
		targetAvgMicros = sumMaxMicros / 2
	}

	if sumMaxMicros > microsScale || sumAbortMicros > microsScale || targetAvgMicros > microsScale {
		return args{}, fmt.Errorf("equal thresholds must be <= 1.0")
	}
	if targetAvgMicros == 0 {
		return args{}, fmt.Errorf("ARBITRAGE_EQUAL_TARGET_AVG must be > 0")
	}
	if sumAbortMicros < sumMaxMicros {
		return args{}, fmt.Errorf("ARBITRAGE_EQUAL_SUM_ABORT must be >= ARBITRAGE_EQUAL_SUM_MAX")
	}

	parsed.equalTargetAvgMicros = targetAvgMicros
	parsed.equalSumAbortMicros = sumAbortMicros

	parsed.orderType = clob.OrderTypeFAK
	parsed.enableSell = false

	return parsed, nil
}

func stepEqualBuys(
	ctx context.Context,
	clobClient *clob.Client,
	parsed args,
	st *botState,
	bestA, bestB arbitrage.BestAsk,
	asksA, asksB []arbitrage.Level,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	windowStartsAt time.Time,
) {
	if st == nil || st.closed {
		return
	}

	now := time.Now()
	priceKeyA := priceChangeKeyMicros(bestA.PriceMicros)
	priceKeyB := priceChangeKeyMicros(bestB.PriceMicros)
	priceChangedA := priceKeyA > 0 && priceKeyA != st.lastBestAskA
	priceChangedB := priceKeyB > 0 && priceKeyB != st.lastBestAskB
	st.lastBestAskA = priceKeyA
	st.lastBestAskB = priceKeyB
	if bestA.PriceMicros > 0 && bestA.SharesMicros > 0 {
		st.a.lastAskPriceMicros = bestA.PriceMicros
		st.a.lastAskSharesMicros = bestA.SharesMicros
	}
	if bestB.PriceMicros > 0 && bestB.SharesMicros > 0 {
		st.b.lastAskPriceMicros = bestB.PriceMicros
		st.b.lastAskSharesMicros = bestB.SharesMicros
	}

	startAt := windowStartsAt
	if startAt.IsZero() {
		startAt = runStartedAt
	}
	if now.Before(startAt) {
		setEqualStatus(st, "waiting for window start", "idle", "start delay")
		return
	}
	if parsed.equalStartDelay > 0 && now.Before(startAt.Add(parsed.equalStartDelay)) {
		setEqualStatus(st, "waiting for start delay", "idle", "monitoring books")
		return
	}

	if parsed.enableTrading && !st.buyCooldownUntil.IsZero() && now.Before(st.buyCooldownUntil) {
		setEqualStatus(st, "buys paused (balance/allowance cooldown)", "idle", "cooldown expires")
		return
	}

	if bestA.PriceMicros == 0 || bestB.PriceMicros == 0 || bestA.SharesMicros == 0 || bestB.SharesMicros == 0 {
		setEqualStatus(st, "waiting for asks", "idle", "monitoring books")
		return
	}

	shouldAttempt := priceChangedA || priceChangedB || st.equal.lastAttemptAt.IsZero() || now.Sub(st.equal.lastAttemptAt) >= equalRetryMinInterval
	if !shouldAttempt {
		setEqualStatus(st, "waiting to retry equalization", "idle", "next retry window")
		return
	}

	if !st.equal.targetSet {
		targetAvg := parsed.equalTargetAvgMicros
		if targetAvg == 0 && parsed.equalSumMaxMicros > 0 {
			targetAvg = parsed.equalSumMaxMicros / 2
		}
		targetShares := mulDivU64(parsed.equalAssignedBudgetMicros, microsScale, targetAvg)
		if targetShares == 0 {
			setEqualStatus(st, "assigned budget too small", "idle", "waiting")
			return
		}
		st.equal.targetSharesMicros = targetShares
		st.equal.targetAvgMicros = targetAvg
		st.equal.targetSet = true
		st.equal.baseDone = false
		setEqualStatus(
			st,
			fmt.Sprintf("target set (shares=%s avg=%s)", formatShares(targetShares), formatMicros(targetAvg)),
			"idle",
			"placing assigned buys",
		)
	}

	if !st.equal.baseDone {
		baseBudgetCap := parsed.equalAssignedBudgetMicros * 2
		stageBudgetCap := addBudgetOverage(baseBudgetCap)
		totalBudgetCap := baseBudgetCap
		totalTarget := st.equal.targetSharesMicros
		sumCapSpend := mulDivU64(totalTarget, parsed.equalSumMaxMicros, microsScale)
		totalSpendCap := minU64(totalBudgetCap, sumCapSpend)
		totalSpendCap = addBudgetOverage(totalSpendCap)
		strictNoDrift := equalStrictNoDrift(now, parsed, windowSlug, windowStartsAt)
		attempted, done, reason := equalizeAverageToTarget(
			ctx,
			clobClient,
			parsed,
			st,
			bestA,
			bestB,
			totalTarget,
			stageBudgetCap,
			&st.equal.assignedSpentMicros,
			totalSpendCap,
			strictNoDrift,
			saltGen,
			tradeLog,
			runStartedAt,
			windowSlug,
			"assigned",
		)
		if attempted {
			st.equal.lastAttemptAt = now
		}
		if !done {
			mode := "equalizing assigned shares"
			if strictNoDrift {
				mode = "equalizing assigned shares (no drift)"
			}
			setEqualStatus(
				st,
				fmt.Sprintf("%s (%s)", mode, reason),
				"placing assigned buys",
				"retrying equalization",
			)
			return
		}
		st.equal.baseDone = true
		setEqualStatus(st, fmt.Sprintf("assigned entry complete (%s)", reason), "idle", "holding position")
		return
	}
}

func equalizeAverageToTarget(
	ctx context.Context,
	clobClient *clob.Client,
	parsed args,
	st *botState,
	bestA, bestB arbitrage.BestAsk,
	targetShares uint64,
	stageBudgetCap uint64,
	stageSpent *uint64,
	totalSpendCap uint64,
	strictNoDrift bool,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	budgetLabel string,
) (bool, bool, string) {
	if targetShares == 0 {
		return false, true, "target=0"
	}
	if st == nil || stageSpent == nil {
		return false, true, "state missing"
	}

	defA := deficitShares(st.a.heldSharesMicros, targetShares)
	defB := deficitShares(st.b.heldSharesMicros, targetShares)
	if defA == 0 && defB == 0 {
		return false, true, "target met"
	}

	avgA := equalAvgMicros(&st.a)
	avgB := equalAvgMicros(&st.b)
	avgSum := avgA + avgB

	budgetAvailable := func() uint64 {
		totalSpent := st.equal.assignedSpentMicros
		totalRemaining := remainingBudget(totalSpendCap, totalSpent)
		stageRemaining := remainingBudget(stageBudgetCap, *stageSpent)
		return minU64(totalRemaining, stageRemaining)
	}

	totalSpent := st.equal.assignedSpentMicros
	totalRemaining := remainingBudget(totalSpendCap, totalSpent)
	if totalRemaining < minBuyMicros {
		return false, true, equalReasonWithMetrics("budget exhausted", avgSum, 0, 0, totalRemaining, false, false)
	}

	minSharesA := minBuyShares(bestA.PriceMicros)
	minSharesB := minBuyShares(bestB.PriceMicros)

	sizeBlockedA := defA > 0 && (minSharesA == 0 || bestA.SharesMicros < minSharesA)
	sizeBlockedB := defB > 0 && (minSharesB == 0 || bestB.SharesMicros < minSharesB)

	maxPriceA, okA := equalMaxPrice(totalRemaining, defA, defB, bestB.PriceMicros)
	maxPriceB, okB := equalMaxPrice(totalRemaining, defB, defA, bestA.PriceMicros)

	allowA := defA > 0 && okA && bestA.PriceMicros <= maxPriceA && !sizeBlockedA
	allowB := defB > 0 && okB && bestB.PriceMicros <= maxPriceB && !sizeBlockedB

	if !allowA && !allowB {
		if sizeBlockedA || sizeBlockedB {
			return false, false, equalReasonWithMetrics("waiting for size", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
		}
		return false, false, equalReasonWithMetrics("waiting for price cap", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
	}

	attempted := false
	tryBuy := func(s *sideState, best arbitrage.BestAsk, need uint64, minShares uint64, label string) bool {
		if need == 0 {
			return false
		}
		if best.SharesMicros > 0 && need > best.SharesMicros {
			need = best.SharesMicros
		}
		if minShares > 0 && need < minShares {
			return false
		}
		avail := budgetAvailable()
		if avail < minBuyMicros {
			return false
		}
		budgetRemaining := avail
		_, spent, err := attemptEqualBuy(
			ctx,
			clobClient,
			parsed,
			st,
			s,
			best.PriceMicros,
			need,
			&budgetRemaining,
			stageSpent,
			nil,
			saltGen,
			tradeLog,
			runStartedAt,
			windowSlug,
			budgetLabel,
			label,
		)
		if err != nil {
			return false
		}
		if spent > 0 {
			attempted = true
		}
		return spent > 0
	}

	if strictNoDrift {
		switch {
		case defA > defB:
			if !allowA {
				return attempted, false, equalReasonWithMetrics("waiting for price cap", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			maxCatchUp := defA - defB
			if maxCatchUp < minSharesA || maxCatchUp == 0 {
				return attempted, false, equalReasonWithMetrics("waiting for size", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			tryBuy(&st.a, bestA, maxCatchUp, minSharesA, "a")
		case defB > defA:
			if !allowB {
				return attempted, false, equalReasonWithMetrics("waiting for price cap", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			maxCatchUp := defB - defA
			if maxCatchUp < minSharesB || maxCatchUp == 0 {
				return attempted, false, equalReasonWithMetrics("waiting for size", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			tryBuy(&st.b, bestB, maxCatchUp, minSharesB, "b")
		default:
			if !allowA || !allowB {
				if sizeBlockedA || sizeBlockedB {
					return attempted, false, equalReasonWithMetrics("waiting for size", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
				}
				return attempted, false, equalReasonWithMetrics("waiting for price cap", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			avail := budgetAvailable()
			priceSum := bestA.PriceMicros + bestB.PriceMicros
			if avail < minBuyMicros || priceSum == 0 {
				return attempted, false, equalReasonWithMetrics("waiting for budget", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			maxEqualShares := mulDivU64(avail, microsScale, priceSum)
			if bestA.SharesMicros > 0 && maxEqualShares > bestA.SharesMicros {
				maxEqualShares = bestA.SharesMicros
			}
			if bestB.SharesMicros > 0 && maxEqualShares > bestB.SharesMicros {
				maxEqualShares = bestB.SharesMicros
			}
			if maxEqualShares > defA {
				maxEqualShares = defA
			}
			minEqual := maxU64(minSharesA, minSharesB)
			if maxEqualShares < minEqual || maxEqualShares == 0 {
				return attempted, false, equalReasonWithMetrics("waiting for size", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
			}
			budgetRemaining := avail
			_, spentA, errA := attemptEqualBuy(
				ctx,
				clobClient,
				parsed,
				st,
				&st.a,
				bestA.PriceMicros,
				maxEqualShares,
				&budgetRemaining,
				stageSpent,
				nil,
				saltGen,
				tradeLog,
				runStartedAt,
				windowSlug,
				budgetLabel,
				"a",
			)
			if errA == nil && spentA > 0 {
				attempted = true
			}
			_, spentB, errB := attemptEqualBuy(
				ctx,
				clobClient,
				parsed,
				st,
				&st.b,
				bestB.PriceMicros,
				maxEqualShares,
				&budgetRemaining,
				stageSpent,
				nil,
				saltGen,
				tradeLog,
				runStartedAt,
				windowSlug,
				budgetLabel,
				"b",
			)
			if errB == nil && spentB > 0 {
				attempted = true
			}
		}
	} else {
		drift := absDiffU64(st.a.heldSharesMicros, st.b.heldSharesMicros)
		driftCap := maxU64(minSharesA, minSharesB)
		if defA > defB && drift > driftCap {
			allowB = false
		} else if defB > defA && drift > driftCap {
			allowA = false
		}

		order := []struct {
			side  *sideState
			best  arbitrage.BestAsk
			need  uint64
			label string
			allow bool
			min   uint64
		}{
			{side: &st.a, best: bestA, need: defA, label: "a", allow: allowA, min: minSharesA},
			{side: &st.b, best: bestB, need: defB, label: "b", allow: allowB, min: minSharesB},
		}
		if defB > defA {
			order[0], order[1] = order[1], order[0]
		}
		for _, o := range order {
			if !o.allow {
				continue
			}
			tryBuy(o.side, o.best, o.need, o.min, o.label)
		}
	}

	defA = deficitShares(st.a.heldSharesMicros, targetShares)
	defB = deficitShares(st.b.heldSharesMicros, targetShares)
	if defA == 0 && defB == 0 {
		return attempted, true, equalReasonWithMetrics("target met", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
	}
	totalSpent = st.equal.assignedSpentMicros
	totalRemaining = remainingBudget(totalSpendCap, totalSpent)
	if totalRemaining < minBuyMicros {
		return attempted, true, equalReasonWithMetrics("budget exhausted", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
	}
	costA := uint64(0)
	if defA > 0 {
		costA = arbitrage.CostMicrosForShares(defA, bestA.PriceMicros)
	}
	costB := uint64(0)
	if defB > 0 {
		costB = arbitrage.CostMicrosForShares(defB, bestB.PriceMicros)
	}
	if (defA == 0 || costA < minBuyMicros) && (defB == 0 || costB < minBuyMicros) {
		return attempted, true, equalReasonWithMetrics("remaining below min order", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
	}
	return attempted, false, equalReasonWithMetrics("retry", avgSum, maxPriceA, maxPriceB, totalRemaining, okA, okB)
}

func attemptEqualBuy(
	ctx context.Context,
	clobClient *clob.Client,
	parsed args,
	st *botState,
	s *sideState,
	priceMicros uint64,
	sharesNeeded uint64,
	budgetRemaining *uint64,
	budgetSpent *uint64,
	overallSpent *uint64,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	budgetLabel string,
	sideLabel string,
) (uint64, uint64, error) {
	if s == nil || priceMicros == 0 || sharesNeeded == 0 || budgetRemaining == nil || *budgetRemaining == 0 {
		return 0, 0, nil
	}

	maxShares := arbitrage.MaxSharesFromCapMicros(*budgetRemaining, priceMicros)
	if maxShares == 0 {
		return 0, 0, nil
	}
	if maxShares < sharesNeeded {
		sharesNeeded = maxShares
	}
	cost := arbitrage.CostMicrosForShares(sharesNeeded, priceMicros)
	if cost < minBuyMicros {
		return 0, 0, nil
	}

	beforeSpent := s.spentMicros
	beforeShares := s.totalBoughtSharesMicros

	ok := true
	var errMsg string
	if parsed.enableTrading {
		if _, err := executeExactBuy(ctx, clobClient, s, cost, sharesNeeded, clob.OrderTypeFAK, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, "equalize_target_avg"); err != nil {
			ok = false
			errMsg = err.Error()
			if isNotEnoughBalanceOrAllowance(errMsg) && parsed.balanceAllowanceCooldown > 0 {
				st.buyCooldownUntil = time.Now().Add(parsed.balanceAllowanceCooldown)
				log.Printf("[warn] pausing buys for %s due to balance/allowance (until %s)", parsed.balanceAllowanceCooldown, st.buyCooldownUntil.Format(time.RFC3339))
				logBalanceAllowanceSnapshot(ctx, clobClient, st, cost)
			}
		} else {
			log.Printf("[equal] buy budget=%s side=%s price=%s shares=%s", budgetLabel, sideLabel, formatMicros(priceMicros), formatShares(sharesNeeded))
		}
	} else {
		applyDryRunBuyLot(s, priceMicros, sharesNeeded, cost)
		log.Printf("[dry] equal buy budget=%s side=%s price=%s shares=%s cost=%s", budgetLabel, sideLabel, formatMicros(priceMicros), formatShares(sharesNeeded), formatMicros(cost))
	}

	afterSpent := s.spentMicros
	afterShares := s.totalBoughtSharesMicros
	deltaSpent := afterSpent - beforeSpent
	deltaShares := afterShares - beforeShares
	if deltaSpent > *budgetRemaining {
		deltaSpent = *budgetRemaining
	}
	*budgetRemaining -= deltaSpent
	if budgetSpent != nil {
		*budgetSpent += deltaSpent
	}
	if overallSpent != nil {
		*overallSpent += deltaSpent
	}

	eventCost := cost
	eventShares := sharesNeeded
	if deltaSpent > 0 || deltaShares > 0 {
		eventCost = deltaSpent
		eventShares = deltaShares
	}

	logArbEvent(tradeLog, arbLogEvent{
		TsMs:          time.Now().UnixMilli(),
		Event:         fmt.Sprintf("buy_equal_%s", budgetLabel),
		Mode:          arbMode(parsed.enableTrading),
		Source:        parsed.source,
		EventSlug:     parsed.eventSlug,
		WindowSlug:    windowSlug,
		TokenID:       s.tokenID,
		MakerMicros:   eventCost,
		SharesMicros:  eventShares,
		Price:         formatMicros(priceMicros),
		OrderType:     string(clob.OrderTypeFAK),
		Reason:        "equalize_target_avg",
		EnableTrading: parsed.enableTrading,
		Ok:            ok,
		Err:           errMsg,
		UptimeMs:      uptimeMs(runStartedAt),
	})

	return deltaShares, deltaSpent, nil
}

func deficitShares(current, target uint64) uint64 {
	if target == 0 || current >= target {
		return 0
	}
	return target - current
}

func clearEqualStateIfFlat(st *botState) {
	if st == nil {
		return
	}
	if st.a.heldSharesMicros == 0 && st.b.heldSharesMicros == 0 {
		st.equal = equalState{}
	}
}

func equalStatusTracker(st *botState) *statusTracker {
	if st == nil {
		return nil
	}
	if st.equal.status.slots == nil {
		st.equal.status = newStatusTracker("[equal]", equalStatusMinInterval)
	}
	return &st.equal.status
}

func setEqualStatus(st *botState, intent, doing, next string) {
	tracker := equalStatusTracker(st)
	if tracker == nil {
		return
	}
	if intent == "" {
		intent = "idle"
	}
	if doing == "" {
		doing = "idle"
	}
	if next == "" {
		next = "idle"
	}
	tracker.Set("intent", intent)
	tracker.Set("doing", doing)
	tracker.Set("next", next)
}

func equalStrictNoDrift(now time.Time, parsed args, windowSlug string, windowStartsAt time.Time) bool {
	slug := strings.TrimSpace(windowSlug)
	if slug == "" {
		slug = strings.TrimSpace(parsed.eventSlug)
	}
	if slug == "" {
		return false
	}
	ws, err := parseWindowedSlug(slug)
	if err != nil || ws.interval <= 0 {
		return false
	}
	startAt := windowStartsAt
	if startAt.IsZero() {
		locName := strings.TrimSpace(parsed.eventTZ)
		if locName == "" {
			locName = "America/New_York"
		}
		loc, err := time.LoadLocation(locName)
		if err != nil {
			return false
		}
		startAt = windowStartForNow(now, ws, loc)
	}
	if startAt.IsZero() {
		return false
	}
	endAt := startAt.Add(ws.interval)
	remaining := endAt.Sub(now)
	return remaining > 0 && remaining <= 3*time.Minute
}

func addBudgetOverage(capMicros uint64) uint64 {
	if capMicros > ^uint64(0)-minBuyMicros {
		return ^uint64(0)
	}
	return capMicros + minBuyMicros
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func absDiffU64(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

func minBuyShares(priceMicros uint64) uint64 {
	if priceMicros == 0 {
		return 0
	}
	return mulDivCeilU64(minBuyMicros, microsScale, priceMicros)
}

func equalMaxPrice(remainingBudget, remainingSharesSide, remainingSharesOther, otherBestPrice uint64) (uint64, bool) {
	if remainingSharesSide == 0 {
		return 0, false
	}
	if remainingSharesOther > 0 && otherBestPrice == 0 {
		return 0, false
	}
	costOther := uint64(0)
	if remainingSharesOther > 0 {
		costOther = arbitrage.CostMicrosForShares(remainingSharesOther, otherBestPrice)
	}
	if remainingBudget <= costOther {
		return 0, false
	}
	budgetForSide := remainingBudget - costOther
	maxPrice := mulDivU64(budgetForSide, microsScale, remainingSharesSide)
	if maxPrice == 0 {
		return 0, false
	}
	return maxPrice, true
}

func equalAvgMicros(s *sideState) uint64 {
	if s == nil || s.totalBoughtSharesMicros == 0 {
		return 0
	}
	return mulDivU64(s.totalBoughtMicros, microsScale, s.totalBoughtSharesMicros)
}

func equalReasonWithMetrics(reason string, avgSum, maxPriceA, maxPriceB, remaining uint64, okA, okB bool) string {
	maxA := "n/a"
	maxB := "n/a"
	if okA && maxPriceA > 0 {
		maxA = formatMicros(maxPriceA)
	}
	if okB && maxPriceB > 0 {
		maxB = formatMicros(maxPriceB)
	}
	return fmt.Sprintf("%s avg_sum=%s max_a=%s max_b=%s remaining=%s", reason, formatMicros(avgSum), maxA, maxB, formatMicros(remaining))
}
