package arbbot

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"poly-gocopy/internal/arbitrage"
	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/jsonl"
)

const (
	weightedStatusMinInterval = 2 * time.Second
)

type weightedState struct {
	lastAttemptAt time.Time
	status        statusTracker

	recoveryOrderID      string
	recoverySide         rangeSide
	recoveryPriceMicros  uint64
	recoverySharesMicros uint64
	recoveryLastCheckAt  time.Time
}

func RunWeighted() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	parsed, err := parseWeightedArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	runStrategy(parsed, "weighted", stepWeightedBuys, logWeightedConfig, startWeightedClaimLoop)
}

func startWeightedClaimLoop(ctx context.Context) {
	enableRaw := strings.TrimSpace(os.Getenv("ARBITRAGE_WEIGHTED_CLAIM_ENABLE"))
	enabled := true
	if enableRaw != "" {
		parsed, err := strconv.ParseBool(enableRaw)
		if err != nil {
			log.Printf("[warn] invalid ARBITRAGE_WEIGHTED_CLAIM_ENABLE %q: %v", enableRaw, err)
			return
		}
		enabled = parsed
	}
	if !enabled {
		log.Printf("[claim] weighted relayer: disabled (ARBITRAGE_WEIGHTED_CLAIM_ENABLE=false)")
		return
	}

	cmdStr := strings.TrimSpace(os.Getenv("ARBITRAGE_WEIGHTED_CLAIM_CMD"))
	if cmdStr == "" {
		cmdStr = "uv run scripts/claim_relayer.py"
	}

	every := 5 * time.Minute
	if raw := strings.TrimSpace(os.Getenv("ARBITRAGE_WEIGHTED_CLAIM_EVERY")); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil {
			log.Printf("[warn] invalid ARBITRAGE_WEIGHTED_CLAIM_EVERY %q: %v", raw, err)
			return
		}
		if v <= 0 {
			log.Printf("[warn] ARBITRAGE_WEIGHTED_CLAIM_EVERY must be > 0")
			return
		}
		every = v
	}

	log.Printf("[claim] weighted relayer: every=%s cmd=%q", every, cmdStr)

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		if err := runWeightedClaimCommand(ctx, cmdStr, every); err != nil {
			log.Printf("[warn] weighted claim run failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runWeightedClaimCommand(ctx context.Context, cmdStr string, timeout time.Duration) error {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return fmt.Errorf("claim cmd empty")
	}
	cmdCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(cmdCtx, parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func logWeightedConfig(parsed args) {
	log.Printf(
		"Weighted entry: total=%s entry_pct=%.2f%% sum_max<=%s loop=%s start_delay=%s",
		formatMicros(parsed.weightedTotalBudgetMicros),
		float64(parsed.weightedEntryPctMicros)/10000.0,
		formatMicros(parsed.weightedSumMaxMicros),
		parsed.weightedLoopInterval,
		parsed.weightedStartDelay,
	)
	log.Printf(
		"Weighted ranges: A=%s-%s B=%s-%s lead=max 1 batch",
		formatMicros(parsed.weightedAMinMicros),
		formatMicros(parsed.weightedAMaxMicros),
		formatMicros(parsed.weightedBMinMicros),
		formatMicros(parsed.weightedBMaxMicros),
	)
	log.Printf("Weighted buys: order_type=FAK (fixed)")
	log.Printf("Weighted recovery: disabled")
	log.Printf("Weighted sells: disabled (holds positions)")
}

func parseWeightedArgs() (args, error) {
	parsed, err := parseArgs()
	if err != nil {
		return args{}, err
	}

	parsed.rangeStopLossAfter = 0
	parsed.rangeStopLossBudgetMicros = 0
	parsed.rangeStopLossMaxPriceMicros = 0
	parsed.rangeStopLossMarket = false
	parsed.equalAssignedBudgetMicros = 0
	parsed.equalTargetAvgMicros = 0
	parsed.equalSumMaxMicros = 0
	parsed.equalSumAbortMicros = 0
	parsed.enableSell = false
	parsed.orderType = clob.OrderTypeFAK

	totalRaw := strings.TrimSpace(os.Getenv("ARBITRAGE_WEIGHTED_TOTAL_USD"))
	if totalRaw == "" {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_TOTAL_USD is required")
	}
	totalMicros, err := arbitrage.ParseMicros(totalRaw)
	if err != nil || totalMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_TOTAL_USD %q", totalRaw)
	}
	if totalMicros < minBuyMicros*2 {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_TOTAL_USD must be >= %s", formatMicros(minBuyMicros*2))
	}
	parsed.weightedTotalBudgetMicros = totalMicros

	entryPctRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_ENTRY_PCT"), "0.10"))
	entryPctMicros, err := parseEntryPctMicros(entryPctRaw)
	if err != nil {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_ENTRY_PCT %q: %w", entryPctRaw, err)
	}
	parsed.weightedEntryPctMicros = entryPctMicros

	sumMaxRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_SUM_MAX"), "0.97"))
	sumMaxMicros, err := arbitrage.ParseMicros(sumMaxRaw)
	if err != nil || sumMaxMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_SUM_MAX %q", sumMaxRaw)
	}
	if sumMaxMicros > microsScale {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_SUM_MAX must be <= 1.0")
	}
	parsed.weightedSumMaxMicros = sumMaxMicros

	loopRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_LOOP_INTERVAL"), "1s"))
	loopInterval, err := time.ParseDuration(loopRaw)
	if err != nil {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_LOOP_INTERVAL %q: %w", loopRaw, err)
	}
	if loopInterval <= 0 {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_LOOP_INTERVAL must be > 0")
	}
	parsed.weightedLoopInterval = loopInterval

	startDelayRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_START_DELAY"), "30s"))
	startDelay, err := time.ParseDuration(startDelayRaw)
	if err != nil {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_START_DELAY %q: %w", startDelayRaw, err)
	}
	if startDelay < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_START_DELAY must be >= 0")
	}
	parsed.weightedStartDelay = startDelay

	aMinRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_A_MIN"), "0.52"))
	aMaxRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_A_MAX"), "0.90"))
	bMinRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_B_MIN"), "0.01"))
	bMaxRaw := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WEIGHTED_B_MAX"), "0.46"))

	aMinMicros, err := arbitrage.ParseMicros(aMinRaw)
	if err != nil || aMinMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_A_MIN %q", aMinRaw)
	}
	aMaxMicros, err := arbitrage.ParseMicros(aMaxRaw)
	if err != nil || aMaxMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_A_MAX %q", aMaxRaw)
	}
	if aMinMicros > aMaxMicros {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_A_MIN must be <= ARBITRAGE_WEIGHTED_A_MAX")
	}
	if aMaxMicros > microsScale {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_A_MAX must be <= 1.0")
	}

	bMinMicros, err := arbitrage.ParseMicros(bMinRaw)
	if err != nil || bMinMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_B_MIN %q", bMinRaw)
	}
	bMaxMicros, err := arbitrage.ParseMicros(bMaxRaw)
	if err != nil || bMaxMicros == 0 {
		return args{}, fmt.Errorf("invalid ARBITRAGE_WEIGHTED_B_MAX %q", bMaxRaw)
	}
	if bMinMicros > bMaxMicros {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_B_MIN must be <= ARBITRAGE_WEIGHTED_B_MAX")
	}
	if bMaxMicros > microsScale {
		return args{}, fmt.Errorf("ARBITRAGE_WEIGHTED_B_MAX must be <= 1.0")
	}

	parsed.weightedAMinMicros = aMinMicros
	parsed.weightedAMaxMicros = aMaxMicros
	parsed.weightedBMinMicros = bMinMicros
	parsed.weightedBMaxMicros = bMaxMicros

	return parsed, nil
}

func parseEntryPctMicros(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("entry pct required")
	}
	isPercent := strings.HasSuffix(s, "%")
	if isPercent {
		s = strings.TrimSuffix(s, "%")
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, err
	}
	if isPercent {
		v = v / 100.0
	} else if v > 1.0 && v <= 100.0 {
		v = v / 100.0
	}
	if v <= 0 || v > 1.0 {
		return 0, fmt.Errorf("entry pct must be > 0 and <= 1.0")
	}
	pctMicros := uint64(math.Round(v * float64(microsScale)))
	if pctMicros == 0 {
		return 0, fmt.Errorf("entry pct too small")
	}
	if pctMicros > microsScale {
		pctMicros = microsScale
	}
	return pctMicros, nil
}

type depthFill struct {
	sharesMicros    uint64
	costMicros      uint64
	avgPriceMicros  uint64
	lastPriceMicros uint64
	levelsUsed      int
}

func fillBySpend(levels []arbitrage.Level, spendCapMicros uint64) depthFill {
	if spendCapMicros == 0 || len(levels) == 0 {
		return depthFill{}
	}
	remaining := spendCapMicros
	var shares uint64
	var cost uint64
	var lastPrice uint64
	levelsUsed := 0
	for _, lvl := range levels {
		if remaining == 0 {
			break
		}
		if lvl.PriceMicros == 0 || lvl.SharesMicros == 0 {
			continue
		}
		levelCost := arbitrage.CostMicrosForShares(lvl.SharesMicros, lvl.PriceMicros)
		if levelCost <= remaining {
			shares += lvl.SharesMicros
			cost += levelCost
			remaining -= levelCost
			lastPrice = lvl.PriceMicros
			levelsUsed++
			continue
		}
		partialShares := arbitrage.MaxSharesFromCapMicros(remaining, lvl.PriceMicros)
		if partialShares == 0 {
			break
		}
		partialCost := arbitrage.CostMicrosForShares(partialShares, lvl.PriceMicros)
		shares += partialShares
		cost += partialCost
		remaining = 0
		lastPrice = lvl.PriceMicros
		levelsUsed++
		break
	}
	avg := uint64(0)
	if shares > 0 {
		avg = mulDivU64(cost, microsScale, shares)
	}
	return depthFill{sharesMicros: shares, costMicros: cost, avgPriceMicros: avg, lastPriceMicros: lastPrice, levelsUsed: levelsUsed}
}

func fillByShares(levels []arbitrage.Level, targetSharesMicros uint64) depthFill {
	if targetSharesMicros == 0 || len(levels) == 0 {
		return depthFill{}
	}
	remaining := targetSharesMicros
	var shares uint64
	var cost uint64
	var lastPrice uint64
	levelsUsed := 0
	for _, lvl := range levels {
		if remaining == 0 {
			break
		}
		if lvl.PriceMicros == 0 || lvl.SharesMicros == 0 {
			continue
		}
		useShares := lvl.SharesMicros
		if useShares > remaining {
			useShares = remaining
		}
		useCost := arbitrage.CostMicrosForShares(useShares, lvl.PriceMicros)
		shares += useShares
		cost += useCost
		remaining -= useShares
		lastPrice = lvl.PriceMicros
		levelsUsed++
	}
	avg := uint64(0)
	if shares > 0 {
		avg = mulDivU64(cost, microsScale, shares)
	}
	return depthFill{sharesMicros: shares, costMicros: cost, avgPriceMicros: avg, lastPriceMicros: lastPrice, levelsUsed: levelsUsed}
}

func filterLevelsByPrice(levels []arbitrage.Level, minMicros, maxMicros uint64) []arbitrage.Level {
	if len(levels) == 0 || minMicros == 0 || maxMicros == 0 || minMicros > maxMicros {
		return nil
	}
	filtered := make([]arbitrage.Level, 0, len(levels))
	for _, lvl := range levels {
		if lvl.PriceMicros == 0 || lvl.SharesMicros == 0 {
			continue
		}
		if lvl.PriceMicros < minMicros || lvl.PriceMicros > maxMicros {
			continue
		}
		filtered = append(filtered, lvl)
	}
	return filtered
}

func maxSharesByAvg(levels []arbitrage.Level, maxAvgMicros uint64) depthFill {
	if maxAvgMicros == 0 || len(levels) == 0 {
		return depthFill{}
	}
	var shares uint64
	var cost uint64
	var lastPrice uint64
	levelsUsed := 0
	for _, lvl := range levels {
		if lvl.PriceMicros == 0 || lvl.SharesMicros == 0 {
			continue
		}
		if shares == 0 {
			if lvl.PriceMicros > maxAvgMicros {
				break
			}
		}
		if lvl.PriceMicros <= maxAvgMicros {
			levelCost := arbitrage.CostMicrosForShares(lvl.SharesMicros, lvl.PriceMicros)
			shares += lvl.SharesMicros
			cost += levelCost
			lastPrice = lvl.PriceMicros
			levelsUsed++
			continue
		}

		maxSharesMul := mulDivU64(maxAvgMicros, shares, 1)
		costMul := mulDivU64(cost, microsScale, 1)
		if maxSharesMul <= costMul {
			break
		}
		numer := maxSharesMul - costMul
		denom := lvl.PriceMicros - maxAvgMicros
		if denom == 0 {
			break
		}
		addShares := numer / denom
		if addShares > lvl.SharesMicros {
			addShares = lvl.SharesMicros
		}
		if addShares == 0 {
			break
		}
		addCost := arbitrage.CostMicrosForShares(addShares, lvl.PriceMicros)
		shares += addShares
		cost += addCost
		lastPrice = lvl.PriceMicros
		levelsUsed++
		break
	}
	avg := uint64(0)
	if shares > 0 {
		avg = mulDivU64(cost, microsScale, shares)
	}
	return depthFill{sharesMicros: shares, costMicros: cost, avgPriceMicros: avg, lastPriceMicros: lastPrice, levelsUsed: levelsUsed}
}

func minOrderSharesFor(s *sideState, priceMicros uint64) uint64 {
	minShares := minBuyShares(priceMicros)
	if s != nil && s.minOrderSharesMicros > minShares {
		minShares = s.minOrderSharesMicros
	}
	return minShares
}

func weightedStatusTracker(st *botState) *statusTracker {
	if st == nil {
		return nil
	}
	if st.weighted.status.slots == nil {
		st.weighted.status = newStatusTracker("[weighted]", weightedStatusMinInterval)
	}
	return &st.weighted.status
}

func setWeightedStatus(st *botState, intent, doing, next string) {
	tracker := weightedStatusTracker(st)
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

func stepWeightedBuys(
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
		setWeightedStatus(st, "waiting for window start", "idle", "start delay")
		return
	}
	if parsed.weightedStartDelay > 0 && now.Before(startAt.Add(parsed.weightedStartDelay)) {
		setWeightedStatus(st, "waiting for start delay", "idle", "monitoring books")
		return
	}

	if parsed.enableTrading && !st.buyCooldownUntil.IsZero() && now.Before(st.buyCooldownUntil) {
		setWeightedStatus(st, "buys paused (balance/allowance cooldown)", "idle", "cooldown expires")
		return
	}

	if len(asksA) == 0 || len(asksB) == 0 {
		setWeightedStatus(st, "waiting for asks", "idle", "monitoring books")
		return
	}

	if !st.weighted.lastAttemptAt.IsZero() && now.Sub(st.weighted.lastAttemptAt) < parsed.weightedLoopInterval {
		setWeightedStatus(st, "waiting for next tick", "idle", "next loop")
		return
	}
	st.weighted.lastAttemptAt = now

	totalSpent := st.a.spentMicros + st.b.spentMicros
	budgetRemaining := remainingBudget(parsed.weightedTotalBudgetMicros, totalSpent)
	if budgetRemaining < minBuyMicros {
		setWeightedStatus(st, "budget exhausted", "idle", "waiting")
		return
	}

	entryBudget := mulDivU64(budgetRemaining, parsed.weightedEntryPctMicros, microsScale)
	if entryBudget < minBuyMicros {
		setWeightedStatus(st, "entry budget below min", "idle", "waiting")
		return
	}

	sharesA := st.a.heldSharesMicros
	sharesB := st.b.heldSharesMicros
	didBuyA := false
	didBuyB := false

	aInRange := priceInRange(bestA.PriceMicros, parsed.weightedAMinMicros, parsed.weightedAMaxMicros)
	if aInRange {
		levelsA := filterLevelsByPrice(asksA, parsed.weightedAMinMicros, parsed.weightedAMaxMicros)
		fillA := fillBySpend(levelsA, entryBudget)
		if fillA.sharesMicros > 0 && priceInRange(fillA.avgPriceMicros, parsed.weightedAMinMicros, parsed.weightedAMaxMicros) {
			minSharesA := minOrderSharesFor(&st.a, fillA.avgPriceMicros)
			if fillA.sharesMicros >= minSharesA {
				if sharesA <= sharesB+fillA.sharesMicros {
					costA := fillA.costMicros
					if costA >= minBuyMicros && costA <= budgetRemaining && costA <= remainingCap(st.a.capMicros, st.a.spentMicros) {
						reason := "dynamic_lead"
						okA := true
						var errMsgA string
						if parsed.enableTrading {
							if _, err := executeMarketBuy(ctx, clobClient, &st.a, costA, parsed.orderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, reason); err != nil {
								okA = false
								errMsgA = err.Error()
								if isNotEnoughBalanceOrAllowance(errMsgA) && parsed.balanceAllowanceCooldown > 0 {
									st.buyCooldownUntil = time.Now().Add(parsed.balanceAllowanceCooldown)
									log.Printf("[warn] pausing buys for %s due to balance/allowance (until %s)", parsed.balanceAllowanceCooldown, st.buyCooldownUntil.Format(time.RFC3339))
									logBalanceAllowanceSnapshot(ctx, clobClient, st, costA)
								}
							}
						} else {
							applyDryRunBuyLot(&st.a, fillA.avgPriceMicros, fillA.sharesMicros, costA)
							log.Printf("[dry] weighted buy A price=%s shares=%s cost=%s", formatMicros(fillA.avgPriceMicros), formatShares(fillA.sharesMicros), formatMicros(costA))
						}

						logArbEvent(tradeLog, arbLogEvent{
							TsMs:          time.Now().UnixMilli(),
							Event:         "buy_weighted",
							Mode:          arbMode(parsed.enableTrading),
							Source:        parsed.source,
							EventSlug:     parsed.eventSlug,
							WindowSlug:    windowSlug,
							TokenID:       st.a.tokenID,
							MakerMicros:   costA,
							SharesMicros:  fillA.sharesMicros,
							Price:         formatMicros(fillA.avgPriceMicros),
							OrderType:     string(parsed.orderType),
							Reason:        reason,
							EnableTrading: parsed.enableTrading,
							Ok:            okA || !parsed.enableTrading,
							Err:           errMsgA,
							UptimeMs:      uptimeMs(runStartedAt),
						})
						didBuyA = okA || !parsed.enableTrading
					}
				}
			}
		}
	}

	if didBuyA {
		sharesA = st.a.heldSharesMicros
		sharesB = st.b.heldSharesMicros
	}

	var maxAllowedB uint64
	if sharesB < sharesA {
		totalSpent = st.a.spentMicros + st.b.spentMicros
		budgetRemaining = remainingBudget(parsed.weightedTotalBudgetMicros, totalSpent)
		if budgetRemaining >= minBuyMicros {
			entryBudget = mulDivU64(budgetRemaining, parsed.weightedEntryPctMicros, microsScale)
			if entryBudget >= minBuyMicros {
				avgA := equalAvgMicros(&st.a)
				if avgA > 0 && parsed.weightedSumMaxMicros > avgA {
					maxAllowedB = parsed.weightedSumMaxMicros - avgA
					maxAllowedB = (maxAllowedB / centMicros) * centMicros
					if maxAllowedB > parsed.weightedBMaxMicros {
						maxAllowedB = parsed.weightedBMaxMicros
					}
					if maxAllowedB >= parsed.weightedBMinMicros {
						levelsB := filterLevelsByPrice(asksB, parsed.weightedBMinMicros, maxAllowedB)
						fillB := fillBySpend(levelsB, entryBudget)
						if fillB.sharesMicros > 0 {
							shortage := sharesA - sharesB
							targetShares := fillB.sharesMicros
							if targetShares > shortage {
								targetShares = shortage
							}
							if targetShares > 0 {
								if targetShares != fillB.sharesMicros {
									fillB = fillByShares(levelsB, targetShares)
								}
								if fillB.sharesMicros > 0 {
									minSharesB := minOrderSharesFor(&st.b, fillB.avgPriceMicros)
									if fillB.sharesMicros >= minSharesB {
										costB := fillB.costMicros
										if costB >= minBuyMicros && costB <= budgetRemaining && costB <= remainingCap(st.b.capMicros, st.b.spentMicros) {
											reason := "dynamic_balance"
											okB := true
											var errMsgB string
											if parsed.enableTrading {
												if _, err := executeMarketBuy(ctx, clobClient, &st.b, costB, parsed.orderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, reason); err != nil {
													okB = false
													errMsgB = err.Error()
													if isNotEnoughBalanceOrAllowance(errMsgB) && parsed.balanceAllowanceCooldown > 0 {
														st.buyCooldownUntil = time.Now().Add(parsed.balanceAllowanceCooldown)
														log.Printf("[warn] pausing buys for %s due to balance/allowance (until %s)", parsed.balanceAllowanceCooldown, st.buyCooldownUntil.Format(time.RFC3339))
														logBalanceAllowanceSnapshot(ctx, clobClient, st, costB)
													}
												}
											} else {
												applyDryRunBuyLot(&st.b, fillB.avgPriceMicros, fillB.sharesMicros, costB)
												log.Printf("[dry] weighted buy B price=%s shares=%s cost=%s", formatMicros(fillB.avgPriceMicros), formatShares(fillB.sharesMicros), formatMicros(costB))
											}

											logArbEvent(tradeLog, arbLogEvent{
												TsMs:          time.Now().UnixMilli(),
												Event:         "buy_weighted",
												Mode:          arbMode(parsed.enableTrading),
												Source:        parsed.source,
												EventSlug:     parsed.eventSlug,
												WindowSlug:    windowSlug,
												TokenID:       st.b.tokenID,
												MakerMicros:   costB,
												SharesMicros:  fillB.sharesMicros,
												Price:         formatMicros(fillB.avgPriceMicros),
												OrderType:     string(parsed.orderType),
												Reason:        reason,
												EnableTrading: parsed.enableTrading,
												Ok:            okB || !parsed.enableTrading,
												Err:           errMsgB,
												UptimeMs:      uptimeMs(runStartedAt),
											})
											didBuyB = okB || !parsed.enableTrading
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if didBuyA || didBuyB {
		setWeightedStatus(st, "entry submitted", "waiting", "next tick")
		return
	}

	if sharesB < sharesA {
		limitLabel := "n/a"
		if maxAllowedB > 0 {
			limitLabel = formatMicros(maxAllowedB)
		}
		bestLabel := "n/a"
		if bestB.PriceMicros > 0 {
			bestLabel = formatMicros(bestB.PriceMicros)
		}
		shortage := sharesA - sharesB
		setWeightedStatus(
			st,
			fmt.Sprintf("waiting for B <= %s (best=%s shortage=%s)", limitLabel, bestLabel, formatShares(shortage)),
			"idle",
			"monitoring books",
		)
		return
	}

	if !aInRange {
		setWeightedStatus(st, "waiting for A in range", "idle", "monitoring books")
		return
	}

	setWeightedStatus(st, "waiting", "idle", "next tick")
}
