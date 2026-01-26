package arbbot

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	orderconfig "github.com/polymarket/go-order-utils/pkg/config"

	"poly-gocopy/internal/arbitrage"
	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/jsonl"
	"poly-gocopy/internal/polygonutil"
	"poly-gocopy/internal/rtds"
)

type args struct {
	tokenA     string
	tokenB     string
	eventSlug  string
	eventSlugs []string
	// Optional path to a plain-text slugs file (1 per line, supports # comments).
	// If set, slugs can be reloaded via SIGHUP without restarting the process.
	eventSlugsFile string
	gammaURL       string
	eventTZ        string

	capAMicros uint64
	capBMicros uint64

	capSplitBalance bool
	capAExplicit    bool
	capBExplicit    bool

	// Range-based entry settings.
	startDelay         time.Duration
	winRangeMinMicros  uint64
	winRangeMaxMicros  uint64
	loseRangeMinMicros uint64
	loseRangeMaxMicros uint64
	// Total per-entry budget for range strategy (split across win/lose sides).
	rangeEntryTotalMicros uint64
	// Range profit target (in bps).
	rangeProfitTargetBps int64
	// Range stop-loss/add strategy settings.
	rangeStopLossAfter          time.Duration
	rangeStopLossBudgetMicros   uint64
	rangeStopLossMaxPriceMicros uint64
	rangeStopLossMarket         bool

	// Equal-shares entry strategy settings.
	equalStartDelay           time.Duration
	equalAssignedBudgetMicros uint64
	equalTargetAvgMicros      uint64
	equalSumMaxMicros         uint64
	equalSumAbortMicros       uint64

	// Weighted-average entry strategy settings.
	weightedTotalBudgetMicros uint64
	weightedEntryPctMicros    uint64
	weightedSumMaxMicros      uint64
	weightedLoopInterval      time.Duration
	weightedStartDelay        time.Duration
	weightedAMinMicros        uint64
	weightedAMaxMicros        uint64
	weightedBMinMicros        uint64
	weightedBMaxMicros        uint64

	pollInterval time.Duration

	source           string
	rtdsURL          string
	rtdsPingInterval time.Duration
	rtdsMinInterval  time.Duration

	outFile string

	clobHost      string
	privateKeyHex string
	funder        common.Address
	signatureType int

	apiKey        string
	apiSecret     string
	apiPassphrase string
	apiNonce      uint64
	useServerTime bool

	orderType     clob.OrderType
	enableTrading bool

	// Sell/exit logic.
	enableSell            bool
	sellEvery             time.Duration
	sellLast              time.Duration
	sellForceLast         time.Duration
	sellMinEarlyProfitBps int
	sellMinLateProfitBps  int
	sellDisableBuysInLast bool
	sellOrderType         clob.OrderType
	sellSlippageBps       int

	// Auto-switch helper: start trading the next rolling window in the last
	// portion of the current window (keeps both sessions running concurrently so
	// the current window can still sell/exit).
	nextSlugLast time.Duration

	// Buy throttling (to avoid spamming the API when the wallet is misconfigured).
	balanceAllowanceCooldown time.Duration
}

type sideState struct {
	tokenID string

	capMicros        uint64
	spentMicros      uint64
	heldSharesMicros uint64

	totalBoughtMicros       uint64
	totalBoughtSharesMicros uint64

	minOrderSharesMicros uint64
	inv                  arbitrage.Inventory

	// rangeRoleLock pins this side to the first range role it entered (win/lose)
	// until inventory is cleared for the current slug.
	rangeRoleLock rangeRole
	// rangeStopLossSpentMicros tracks per-side stop-loss add spend.
	rangeStopLossSpentMicros uint64

	lastBidMicros       uint64
	lastAskPriceMicros  uint64
	lastAskSharesMicros uint64
}

type botState struct {
	a sideState
	b sideState

	closed       bool
	lastSellEval time.Time

	// When set, skip submitting new buy orders until this time.
	buyCooldownUntil time.Time
	// Last time we logged on-chain balance/allowance diagnostics.
	lastBalanceAllowanceLogAt time.Time

	lastBestAskA uint64
	lastBestAskB uint64

	rangeEntryStartedAt   time.Time
	rangeStopLossLastAt   time.Time
	rangeStopLossDisabled bool
	rangeStopLossTick     bool
	rangeExitActive       bool
	rangeWinBoughtSide    rangeSide
	rangeWinBought        bool
	rangeLoseBought       bool

	rangeStatus       string
	rangeStatusLastAt time.Time

	equal    equalState
	weighted weightedState
}

const microsScale = uint64(1_000_000)
const centMicros = uint64(10_000)
const minBuyMicros = microsScale
const defaultArbOutFile = "./out/arbitrage_trades.jsonl"
const minSellSharesMicros = uint64(10_000)
const defaultRangeProfitTargetBps = int64(300)
const stopLossMinBuyMicros = minBuyMicros + centMicros
const stopLossMaxRetries = 3

const (
	rangeEntryWinParts  = 2
	rangeEntryLoseParts = 1
	rangeEntryParts     = rangeEntryWinParts + rangeEntryLoseParts
)

func RunRange() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	parsed, err := parseArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}
	runStrategy(parsed, "range", stepRangeBuys, logRangeConfig, nil)
}

func logRangeConfig(parsed args) {
	log.Printf(
		"Range entry: total=%s split=%d/%d (win/lose)",
		formatMicros(parsed.rangeEntryTotalMicros),
		rangeEntryWinParts,
		rangeEntryLoseParts,
	)
	if parsed.rangeStopLossAfter > 0 {
		log.Printf(
			"Range stop-loss: after=%s target=%.2f%% max_price=%s budget_per_side=%s",
			parsed.rangeStopLossAfter,
			float64(parsed.rangeProfitTargetBps)/100.0,
			formatMicros(parsed.rangeStopLossMaxPriceMicros),
			formatMicros(parsed.rangeStopLossBudgetMicros),
		)
	} else {
		log.Printf("Range stop-loss: disabled")
	}
	log.Printf(
		"Range buys: win=%s-%s lose=%s-%s start_delay=%s",
		formatMicros(parsed.winRangeMinMicros),
		formatMicros(parsed.winRangeMaxMicros),
		formatMicros(parsed.loseRangeMinMicros),
		formatMicros(parsed.loseRangeMaxMicros),
		parsed.startDelay,
	)
}

type buyStepFunc func(
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
)

func runPollLoop(ctx context.Context, clobClient *clob.Client, parsed args, st *botState, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, windowSlug string, windowStartsAt, windowEndsAt time.Time, buyStep buyStepFunc) {
	ticker := time.NewTicker(parsed.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		booksCtx, booksCancel := context.WithTimeout(ctx, 8*time.Second)
		bookA, bookB, err := fetchBooks(booksCtx, clobClient, st.a.tokenID, st.b.tokenID)
		booksCancel()
		if err != nil {
			log.Printf("[warn] fetch books: %v", err)
			continue
		}

		stepOnce(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt, buyStep)
	}
}

func runRTDSLoop(ctx context.Context, clobClient *clob.Client, parsed args, st *botState, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, windowSlug string, windowStartsAt, windowEndsAt time.Time, buyStep buyStepFunc) {
	filtersBytes, err := json.Marshal([]string{parsed.tokenA, parsed.tokenB})
	if err != nil {
		log.Fatalf("[fatal] rtds filters: %v", err)
	}

	msgs, errs := rtds.Start(ctx, parsed.rtdsURL, []rtds.Subscription{
		{
			Topic:   "clob_market",
			Type:    "agg_orderbook",
			Filters: string(filtersBytes),
		},
	}, rtds.Options{
		PingInterval: parsed.rtdsPingInterval,
	})

	type booksCache struct {
		mu sync.RWMutex
		a  *clob.OrderBookSummary
		b  *clob.OrderBookSummary
	}
	var books booksCache

	type evalReq struct {
		full bool // full=true runs buy+pair+sell; full=false runs sell-only
	}

	triggerEval := make(chan evalReq, 1)
	notifyEval := func() { // full eval
		select {
		case triggerEval <- evalReq{full: true}:
		default:
		}
	}
	notifySellEval := func() { // sell-only eval
		select {
		case triggerEval <- evalReq{full: false}:
		default:
		}
	}

	if parsed.enableSell && parsed.sellEvery > 0 {
		ticker := time.NewTicker(parsed.sellEvery)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					notifySellEval()
				}
			}
		}()
	}

	go func() {
		var (
			lastEval    time.Time
			pendingFull bool
			pendingSell bool
			timer       *time.Timer
			timerC      <-chan time.Time
			rangeTimer  *time.Timer
			rangeTimerC <-chan time.Time
		)
		stopTimer := func() {
			if timer == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerC = nil
		}
		startTimer := func(d time.Duration) {
			stopTimer()
			timer = time.NewTimer(d)
			timerC = timer.C
		}

		stopRangeTimer := func() {
			if rangeTimer == nil {
				return
			}
			if !rangeTimer.Stop() {
				select {
				case <-rangeTimer.C:
				default:
				}
			}
			rangeTimerC = nil
		}
		startRangeTimer := func(d time.Duration) {
			stopRangeTimer()
			if d < 0 {
				d = 0
			}
			rangeTimer = time.NewTimer(d)
			rangeTimerC = rangeTimer.C
		}
		rescheduleRangeTimer := func(now time.Time) {
			if parsed.rangeStopLossAfter <= 0 || st == nil {
				stopRangeTimer()
				return
			}
			if st.rangeEntryStartedAt.IsZero() || !st.rangeWinBought || !st.rangeLoseBought || st.rangeStopLossDisabled || rangeStopLossBudgetsExhausted(st, parsed.rangeStopLossBudgetMicros) {
				stopRangeTimer()
				return
			}
			base := st.rangeStopLossLastAt
			if base.IsZero() {
				base = st.rangeEntryStartedAt
			}
			nextAt := base.Add(parsed.rangeStopLossAfter)
			if nextAt.Before(now) {
				nextAt = now
			}
			startRangeTimer(nextAt.Sub(now))
			log.Printf(
				"[range] stop-loss timer scheduled: next=%s after=%s last=%s entry=%s disabled=%v",
				nextAt.Format(time.RFC3339),
				parsed.rangeStopLossAfter,
				formatTimeOrZero(st.rangeStopLossLastAt),
				formatTimeOrZero(st.rangeEntryStartedAt),
				st.rangeStopLossDisabled,
			)
		}

		defer stopTimer()
		defer stopRangeTimer()

		for {
			select {
			case <-ctx.Done():
				return

			case req := <-triggerEval:
				if req.full {
					pendingFull = true
				} else {
					pendingSell = true
				}

				now := time.Now()
				if lastEval.IsZero() || now.Sub(lastEval) >= parsed.rtdsMinInterval {
					runFull := pendingFull
					books.mu.RLock()
					bookA := books.a
					bookB := books.b
					books.mu.RUnlock()
					if bookA != nil && bookB != nil {
						pendingFull = false
						pendingSell = false
						lastEval = now
						if runFull {
							stepOnce(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt, buyStep)
						} else {
							stepSellOnly(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt)
						}
						rescheduleRangeTimer(time.Now())
					}
					continue
				}

				if timerC == nil {
					wait := parsed.rtdsMinInterval - now.Sub(lastEval)
					startTimer(wait)
				}

			case <-timerC:
				timerC = nil
				if !pendingFull && !pendingSell {
					continue
				}
				runFull := pendingFull
				pendingFull = false
				pendingSell = false

				books.mu.RLock()
				bookA := books.a
				bookB := books.b
				books.mu.RUnlock()
				if bookA == nil || bookB == nil {
					continue
				}
				lastEval = time.Now()
				if runFull {
					stepOnce(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt, buyStep)
				} else {
					stepSellOnly(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt)
				}
				rescheduleRangeTimer(time.Now())
			case <-rangeTimerC:
				rangeTimerC = nil
				now := time.Now()
				books.mu.RLock()
				bookA := books.a
				bookB := books.b
				books.mu.RUnlock()
				booksOk := bookA != nil && bookB != nil
				if st != nil {
					log.Printf(
						"[range] stop-loss timer fired: books=%v entry=%s win=%v lose=%v disabled=%v spent=(A=%s,B=%s) last=%s after=%s",
						booksOk,
						formatTimeOrZero(st.rangeEntryStartedAt),
						st.rangeWinBought,
						st.rangeLoseBought,
						st.rangeStopLossDisabled,
						formatMicros(st.a.rangeStopLossSpentMicros),
						formatMicros(st.b.rangeStopLossSpentMicros),
						formatTimeOrZero(st.rangeStopLossLastAt),
						parsed.rangeStopLossAfter,
					)
				} else {
					log.Printf("[range] stop-loss timer fired: books=%v after=%s (no state)", booksOk, parsed.rangeStopLossAfter)
				}
				if bookA != nil && bookB != nil {
					if st != nil {
						st.rangeStopLossTick = true
					}
					lastEval = now
					stepSellOnly(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt)
				} else if st != nil {
					// Consume the tick to keep the stop-loss cadence even without books.
					st.rangeStopLossTick = false
					st.rangeStopLossLastAt = now
				}
				rescheduleRangeTimer(time.Now())
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errs:
			if !ok {
				return
			}
			if err != nil {
				log.Printf("[warn] rtds: %v", err)
			}
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			if msg.Topic != "clob_market" || msg.Type != "agg_orderbook" {
				continue
			}
			payload := bytes.TrimSpace(msg.Payload)
			if len(payload) == 0 {
				continue
			}

			changed := false
			ingest := func(book clob.OrderBookSummary) {
				if book.AssetID != parsed.tokenA && book.AssetID != parsed.tokenB {
					return
				}
				books.mu.Lock()
				if book.AssetID == parsed.tokenA {
					tmp := book
					books.a = &tmp
				} else {
					tmp := book
					books.b = &tmp
				}
				books.mu.Unlock()
				changed = true
			}

			switch payload[0] {
			case '{':
				var book clob.OrderBookSummary
				if err := json.Unmarshal(payload, &book); err != nil {
					log.Printf("[warn] rtds payload decode (object): %v", err)
					continue
				}
				ingest(book)
			case '[':
				// RTDS may send an initial data dump as an array of snapshots.
				var batch []clob.OrderBookSummary
				if err := json.Unmarshal(payload, &batch); err != nil {
					log.Printf("[warn] rtds payload decode (array): %v", err)
					continue
				}
				for i := range batch {
					ingest(batch[i])
				}
			default:
				log.Printf("[warn] rtds payload decode: unexpected json (starts with %q)", payload[0])
				continue
			}

			if changed {
				notifyEval()
			}
		}
	}
}

func stepOnce(ctx context.Context, clobClient *clob.Client, parsed args, st *botState, bookA, bookB *clob.OrderBookSummary, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, windowSlug string, windowStartsAt, windowEndsAt time.Time, buyStep buyStepFunc) {
	if st != nil && st.closed {
		return
	}
	if parsed.enableSell {
		stepSellOnly(ctx, clobClient, parsed, st, bookA, bookB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt, windowEndsAt)
		if st != nil && st.closed {
			return
		}
		if st != nil {
			if st.rangeExitActive {
				return
			}
		}
	}

	asksA, minA, err := asksLevelsFromBook(bookA)
	if err != nil {
		log.Printf("[warn] tokenA asks: %v", err)
		return
	}
	asksB, minB, err := asksLevelsFromBook(bookB)
	if err != nil {
		log.Printf("[warn] tokenB asks: %v", err)
		return
	}
	st.a.minOrderSharesMicros = minA
	st.b.minOrderSharesMicros = minB

	if parsed.sellDisableBuysInLast && !windowEndsAt.IsZero() && parsed.sellLast > 0 {
		rem := time.Until(windowEndsAt)
		if rem > 0 && rem <= parsed.sellLast {
			return
		}
	}

	bestA := bestAskFromLevels(asksA)
	bestB := bestAskFromLevels(asksB)

	if buyStep != nil {
		buyStep(ctx, clobClient, parsed, st, bestA, bestB, asksA, asksB, saltGen, tradeLog, runStartedAt, windowSlug, windowStartsAt)
	}
	printStatus(bestA, bestB, *st, bookA, bookB)
}

type rangeRole uint8

const (
	rangeRoleNone rangeRole = iota
	rangeRoleWin
	rangeRoleLose
)

type rangeSide uint8

const (
	rangeSideNone rangeSide = iota
	rangeSideA
	rangeSideB
)

func rangeRoles(bestA, bestB arbitrage.BestAsk) (rangeRole, rangeRole) {
	if bestA.PriceMicros == 0 || bestB.PriceMicros == 0 {
		return rangeRoleNone, rangeRoleNone
	}
	if bestA.PriceMicros > bestB.PriceMicros {
		return rangeRoleWin, rangeRoleLose
	}
	if bestB.PriceMicros > bestA.PriceMicros {
		return rangeRoleLose, rangeRoleWin
	}
	return rangeRoleNone, rangeRoleNone
}

func rangeRoleLabel(role rangeRole) string {
	switch role {
	case rangeRoleWin:
		return "win"
	case rangeRoleLose:
		return "lose"
	default:
		return "none"
	}
}

func rangeRoleForPrice(priceMicros, winMin, winMax, loseMin, loseMax uint64) rangeRole {
	if priceInRange(priceMicros, winMin, winMax) {
		return rangeRoleWin
	}
	if priceInRange(priceMicros, loseMin, loseMax) {
		return rangeRoleLose
	}
	return rangeRoleNone
}

func lockedRangeRole(s *sideState, current rangeRole) rangeRole {
	if s == nil {
		return rangeRoleNone
	}
	if s.rangeRoleLock != rangeRoleNone {
		return s.rangeRoleLock
	}
	return current
}

func resetRangeRoleIfFlat(s *sideState) {
	if s == nil {
		return
	}
	if s.heldSharesMicros == 0 && s.spentMicros == 0 && s.inv.TotalSharesMicros() == 0 {
		s.rangeRoleLock = rangeRoleNone
	}
}

func priceInRange(priceMicros, minMicros, maxMicros uint64) bool {
	if priceMicros == 0 || minMicros == 0 || maxMicros == 0 {
		return false
	}
	return priceMicros >= minMicros && priceMicros <= maxMicros
}

func rangeEntryBudgets(totalMicros uint64) (uint64, uint64) {
	if totalMicros == 0 {
		return 0, 0
	}
	win := mulDivU64(totalMicros, rangeEntryWinParts, rangeEntryParts)
	if win > totalMicros {
		win = totalMicros
	}
	lose := totalMicros - win
	return win, lose
}

func loseSideWaitReason(s *sideState, side rangeSide, role rangeRole, best arbitrage.BestAsk, loseBudget uint64, winSide rangeSide, loseMin, loseMax uint64) string {
	if s == nil {
		return "no-state"
	}
	if best.PriceMicros == 0 || best.SharesMicros == 0 {
		return "no-asks"
	}
	if !priceInRange(best.PriceMicros, loseMin, loseMax) {
		if role == rangeRoleWin {
			return "in-win-range"
		}
		return "out-of-range"
	}
	if role != rangeRoleLose {
		return fmt.Sprintf("role=%s", rangeRoleLabel(role))
	}
	if side == winSide {
		return "same-side-as-win"
	}
	rem := remainingCap(s.capMicros, s.spentMicros)
	if rem == 0 {
		return "cap=0"
	}
	budget := loseBudget
	if budget > rem {
		budget = rem
	}
	budget = (budget / centMicros) * centMicros
	if budget < minBuyMicros {
		return "budget<min"
	}
	shares := arbitrage.MaxSharesFromCapMicros(budget, best.PriceMicros)
	if shares == 0 {
		return "budget-too-small"
	}
	return "eligible"
}

func loseRangeWaitStatus(parsed args, st *botState, bestA, bestB arbitrage.BestAsk, roleA, roleB rangeRole, loseBudget uint64) string {
	if st == nil {
		return "waiting for other side in lose range"
	}
	aReason := loseSideWaitReason(&st.a, rangeSideA, roleA, bestA, loseBudget, st.rangeWinBoughtSide, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	bReason := loseSideWaitReason(&st.b, rangeSideB, roleB, bestB, loseBudget, st.rangeWinBoughtSide, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	return fmt.Sprintf(
		"waiting for lose side in %s-%s (A=%s B=%s)",
		formatMicros(parsed.loseRangeMinMicros),
		formatMicros(parsed.loseRangeMaxMicros),
		aReason,
		bReason,
	)
}

func priceChangeKeyMicros(priceMicros uint64) uint64 {
	if priceMicros == 0 {
		return 0
	}
	// Range strategy treats sub-cent deltas as noise for price-change triggers.
	return (priceMicros / centMicros) * centMicros
}

func bestAskFromBook(book *clob.OrderBookSummary) (arbitrage.BestAsk, bool) {
	if book == nil {
		return arbitrage.BestAsk{}, false
	}
	bestPrice := uint64(0)
	bestShares := uint64(0)
	for _, a := range book.Asks {
		price, err := arbitrage.ParseMicros(a.Price)
		if err != nil || price == 0 {
			continue
		}
		size, err := arbitrage.ParseMicros(a.Size)
		if err != nil || size == 0 {
			continue
		}
		if bestPrice == 0 || price < bestPrice {
			bestPrice = price
			bestShares = size
		}
	}
	if bestPrice == 0 || bestShares == 0 {
		return arbitrage.BestAsk{}, false
	}
	return arbitrage.BestAsk{PriceMicros: bestPrice, SharesMicros: bestShares}, true
}

func bestBidPriceMicrosFromBook(book *clob.OrderBookSummary) (uint64, bool) {
	if book == nil {
		return 0, false
	}
	minSize := minSellSharesMicros
	if book.MinOrder != "" {
		if v, err := arbitrage.ParseMicros(book.MinOrder); err == nil && v > minSize {
			minSize = v
		}
	}
	best := uint64(0)
	for _, b := range book.Bids {
		price, err := arbitrage.ParseMicros(b.Price)
		if err != nil || price == 0 {
			continue
		}
		size, err := arbitrage.ParseMicros(b.Size)
		if err != nil || size == 0 {
			continue
		}
		if size < minSize {
			continue
		}
		if price > best {
			best = price
		}
	}
	if best == 0 {
		return 0, false
	}
	return best, true
}

func stepRangeBuys(
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

	targetPct := float64(parsed.rangeProfitTargetBps) / 100.0
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
		logRangeStatus(st, "waiting for window start")
		return
	}
	if parsed.startDelay > 0 && now.Before(startAt.Add(parsed.startDelay)) {
		logRangeStatus(st, "waiting for start delay")
		return
	}

	if parsed.enableTrading && !st.buyCooldownUntil.IsZero() && now.Before(st.buyCooldownUntil) {
		logRangeStatus(st, "buys paused (balance/allowance cooldown)")
		return
	}

	if !priceChangedA && !priceChangedB {
		return
	}

	entryTotal := parsed.rangeEntryTotalMicros
	if entryTotal == 0 {
		return
	}
	winBudget, loseBudget := rangeEntryBudgets(entryTotal)
	if winBudget == 0 || loseBudget == 0 {
		return
	}

	aInWinRange := priceInRange(bestA.PriceMicros, parsed.winRangeMinMicros, parsed.winRangeMaxMicros)
	aInLoseRange := priceInRange(bestA.PriceMicros, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	bInWinRange := priceInRange(bestB.PriceMicros, parsed.winRangeMinMicros, parsed.winRangeMaxMicros)
	bInLoseRange := priceInRange(bestB.PriceMicros, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	entryReady := (aInWinRange && bInLoseRange) || (bInWinRange && aInLoseRange)

	roleA := rangeRoleForPrice(bestA.PriceMicros, parsed.winRangeMinMicros, parsed.winRangeMaxMicros, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	roleB := rangeRoleForPrice(bestB.PriceMicros, parsed.winRangeMinMicros, parsed.winRangeMaxMicros, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros)
	if roleA == rangeRoleWin && roleB == rangeRoleWin {
		if bestA.PriceMicros >= bestB.PriceMicros {
			roleB = rangeRoleNone
		} else {
			roleA = rangeRoleNone
		}
	}

	if !st.rangeWinBought && !entryReady {
		logRangeStatus(st, "waiting for both sides in range")
	}

	if !st.rangeWinBought {
		if roleA != rangeRoleWin && roleB != rangeRoleWin {
			logRangeStatus(st, "waiting for winning side in 0.52-0.85")
		}
	} else if !st.rangeLoseBought {
		logRangeStatus(st, loseRangeWaitStatus(parsed, st, bestA, bestB, roleA, roleB, loseBudget))
	}

	markRangeEntry := func() {
		if st.rangeWinBought && st.rangeLoseBought && st.rangeEntryStartedAt.IsZero() {
			st.rangeEntryStartedAt = time.Now()
			st.rangeStopLossLastAt = time.Time{}
			st.rangeStopLossDisabled = false
		}
	}

	buySide := func(s *sideState, side rangeSide, role rangeRole, best arbitrage.BestAsk, priceChanged bool) {
		if s == nil || role == rangeRoleNone || !priceChanged {
			return
		}
		if role == rangeRoleWin {
			if st.rangeWinBought {
				return
			}
			if !priceInRange(best.PriceMicros, parsed.winRangeMinMicros, parsed.winRangeMaxMicros) {
				return
			}
		} else {
			if st.rangeLoseBought {
				return
			}
			if !st.rangeWinBought {
				return
			}
			if st.rangeWinBoughtSide == side {
				return
			}
			if !priceInRange(best.PriceMicros, parsed.loseRangeMinMicros, parsed.loseRangeMaxMicros) {
				return
			}
		}
		if best.PriceMicros == 0 || best.SharesMicros == 0 {
			return
		}

		budget := loseBudget
		if role == rangeRoleWin {
			budget = winBudget
		}
		rem := remainingCap(s.capMicros, s.spentMicros)
		if rem == 0 {
			return
		}
		if budget > rem {
			budget = rem
		}
		budget = (budget / centMicros) * centMicros
		if budget < minBuyMicros {
			return
		}

		shares := best.SharesMicros
		if maxShares := arbitrage.MaxSharesFromCapMicros(budget, best.PriceMicros); maxShares < shares {
			shares = maxShares
		}
		if shares == 0 {
			return
		}

		cost := arbitrage.CostMicrosForShares(shares, best.PriceMicros)
		if cost == 0 {
			return
		}

		ok := true
		var errMsg string

		isWinEntry := role == rangeRoleWin
		orderReason := "range_entry_lose"
		if isWinEntry {
			orderReason = "range_entry_win"
		}
		if parsed.enableTrading {
			if _, err := executeExactBuy(ctx, clobClient, s, cost, shares, parsed.orderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, orderReason); err != nil {
				ok = false
				errMsg = err.Error()
				if isNotEnoughBalanceOrAllowance(errMsg) && parsed.balanceAllowanceCooldown > 0 {
					st.buyCooldownUntil = time.Now().Add(parsed.balanceAllowanceCooldown)
					log.Printf("[warn] pausing buys for %s due to balance/allowance (until %s)", parsed.balanceAllowanceCooldown, st.buyCooldownUntil.Format(time.RFC3339))
					logBalanceAllowanceSnapshot(ctx, clobClient, st, cost)
				}
			} else {
				if isWinEntry {
					st.rangeWinBoughtSide = side
					st.rangeWinBought = true
				} else {
					st.rangeLoseBought = true
				}
				markRangeEntry()
				if st.rangeWinBought && st.rangeLoseBought {
					logRangeStatus(st, fmt.Sprintf("position open; waiting for profit >=%.2f%%", targetPct))
				}
				log.Printf("[range] buy token=%s role=%s price=%s shares=%s cost=%s", s.tokenID, rangeRoleLabel(role), formatMicros(best.PriceMicros), formatShares(shares), formatMicros(cost))
			}
		} else {
			applyDryRunBuyLot(s, best.PriceMicros, shares, cost)
			if isWinEntry {
				st.rangeWinBoughtSide = side
				st.rangeWinBought = true
			} else {
				st.rangeLoseBought = true
			}
			markRangeEntry()
			if st.rangeWinBought && st.rangeLoseBought {
				logRangeStatus(st, fmt.Sprintf("position open; waiting for profit >=%.2f%%", targetPct))
			}
			log.Printf("[dry] range buy token=%s role=%s price=%s shares=%s cost=%s", s.tokenID, rangeRoleLabel(role), formatMicros(best.PriceMicros), formatShares(shares), formatMicros(cost))
		}

		logArbEvent(tradeLog, arbLogEvent{
			TsMs:          time.Now().UnixMilli(),
			Event:         "buy_range",
			Mode:          arbMode(parsed.enableTrading),
			Source:        parsed.source,
			EventSlug:     parsed.eventSlug,
			WindowSlug:    windowSlug,
			TokenID:       s.tokenID,
			MakerMicros:   cost,
			SharesMicros:  shares,
			Price:         formatMicros(best.PriceMicros),
			OrderType:     string(parsed.orderType),
			Reason:        orderReason,
			EnableTrading: parsed.enableTrading,
			Ok:            ok,
			Err:           errMsg,
			UptimeMs:      uptimeMs(runStartedAt),
		})
	}

	tryLoseSide := st.rangeWinBought && !st.rangeLoseBought
	buySide(&st.a, rangeSideA, roleA, bestA, priceChangedA || (tryLoseSide && roleA == rangeRoleLose))
	buySide(&st.b, rangeSideB, roleB, bestB, priceChangedB || (tryLoseSide && roleB == rangeRoleLose))
}

func stepSellOnly(ctx context.Context, clobClient *clob.Client, parsed args, st *botState, bookA, bookB *clob.OrderBookSummary, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, windowSlug string, windowStartsAt, windowEndsAt time.Time) {
	if !parsed.enableSell {
		return
	}
	if st == nil || st.closed || clobClient == nil || bookA == nil || bookB == nil {
		return
	}

	now := time.Now()
	clearRangeDustIfAny(st)

	targetPct := float64(parsed.rangeProfitTargetBps) / 100.0
	minProfitBps := int(parsed.rangeProfitTargetBps)
	var remaining time.Duration
	var remainingMs int64
	if !windowEndsAt.IsZero() {
		remaining = windowEndsAt.Sub(now)
		remainingMs = remaining.Milliseconds()
	}
	forceSell := parsed.sellForceLast > 0 && !windowEndsAt.IsZero() && remaining <= parsed.sellForceLast
	exitNow := forceSell || st.rangeExitActive

	sharesA := st.a.heldSharesMicros
	sharesB := st.b.heldSharesMicros
	if sharesA == 0 && sharesB == 0 {
		return
	}
	sellableA := sharesA
	sellableB := sharesB
	if sellableA > 0 && sellableA < minSellSharesMicros {
		log.Printf("[sell] dust on A below min; skipping sell (shares=%s)", formatShares(sellableA))
		sellableA = 0
	}
	if sellableB > 0 && sellableB < minSellSharesMicros {
		log.Printf("[sell] dust on B below min; skipping sell (shares=%s)", formatShares(sellableB))
		sellableB = 0
	}
	if sellableA == 0 && sellableB == 0 && exitNow {
		if sharesA > 0 || sharesB > 0 {
			log.Printf("[sell] clearing dust below min (A=%s, B=%s)", formatShares(sharesA), formatShares(sharesB))
		}
		clearSideAfterSell(&st.a)
		clearSideAfterSell(&st.b)
		clearRangeDustIfAny(st)
		return
	}
	stopLossTick := false
	if st.rangeStopLossTick {
		stopLossTick = true
		st.rangeStopLossTick = false
	}
	shouldCheckStopLoss := parsed.rangeStopLossAfter > 0 &&
		!st.rangeExitActive &&
		!st.rangeStopLossDisabled &&
		!rangeStopLossBudgetsExhausted(st, parsed.rangeStopLossBudgetMicros) &&
		!st.rangeEntryStartedAt.IsZero() &&
		now.Sub(st.rangeEntryStartedAt) >= parsed.rangeStopLossAfter &&
		(stopLossTick || st.rangeStopLossLastAt.IsZero() || now.Sub(st.rangeStopLossLastAt) >= parsed.rangeStopLossAfter)
	st.lastSellEval = now
	sellOrderType := parsed.sellOrderType
	if forceSell {
		// Force sells should be aggressive/partial-fill friendly.
		sellOrderType = clob.OrderTypeFAK
	}
	if exitNow {
		sellOrderType = clob.OrderTypeFAK
	}

	bestBidA, okBidA := bestBidPriceMicrosFromBook(bookA)
	bestBidB, okBidB := bestBidPriceMicrosFromBook(bookB)
	if okBidA {
		st.a.lastBidMicros = bestBidA
	}
	if okBidB {
		st.b.lastBidMicros = bestBidB
	}
	missingBid := (sellableA > 0 && !okBidA) || (sellableB > 0 && !okBidB)
	positionValueMicros := uint64(0)
	if sellableA > 0 && okBidA {
		positionValueMicros += mulDivU64(sellableA, bestBidA, microsScale)
	}
	if sellableB > 0 && okBidB {
		positionValueMicros += mulDivU64(sellableB, bestBidB, microsScale)
	}

	invested := st.a.spentMicros + st.b.spentMicros
	pnlBps := int64(0)
	if invested > 0 && !missingBid {
		pnlBps = pnlBpsFromMicros(invested, positionValueMicros)
		log.Printf(
			"[pnl] value=%s invested=%s pnl_pct=%.4f%% bids=(A=%s,B=%s) remaining=%s",
			formatMicros(positionValueMicros),
			formatMicros(invested),
			float64(pnlBps)/100.0,
			formatMicros(bestBidA),
			formatMicros(bestBidB),
			remaining,
		)
	} else {
		log.Printf(
			"[pnl] unavailable value=%s invested=%s bids=(A=%s,B=%s) remaining=%s",
			formatMicros(positionValueMicros),
			formatMicros(invested),
			formatMicros(bestBidA),
			formatMicros(bestBidB),
			remaining,
		)
	}

	if st.rangeWinBought && st.rangeLoseBought {
		if pnlBps >= parsed.rangeProfitTargetBps {
			logRangeStatus(st, fmt.Sprintf("profit >=%.2f%% hit; selling now", targetPct))
		} else if !st.rangeEntryStartedAt.IsZero() && now.Sub(st.rangeEntryStartedAt) < parsed.rangeStopLossAfter {
			logRangeStatus(st, fmt.Sprintf("waiting for stop-loss timer (%s)", parsed.rangeStopLossAfter))
		} else {
			logRangeStatus(st, fmt.Sprintf("waiting for profit >=%.2f%% (stop-loss checks every %s)", targetPct, parsed.rangeStopLossAfter))
		}
	}

	if shouldCheckStopLoss {
		if stopLossTick {
			// Tick-driven checks are the authoritative stop-loss schedule.
			st.rangeStopLossLastAt = now
		}
		if invested == 0 {
			// Skip stop-loss checks when the position isn't valued yet.
			logRangeStatus(st, "waiting for position cost to evaluate stop-loss")
		} else if pnlBps >= parsed.rangeProfitTargetBps {
			// Already at target profit; no stop-loss adds.
		} else if parsed.enableTrading && !st.buyCooldownUntil.IsZero() && now.Before(st.buyCooldownUntil) {
			// Respect global buy cooldowns.
			logRangeStatus(st, "stop-loss delayed (buy cooldown)")
		} else {
			var finalWinSide *sideState
			var finalWinBest arbitrage.BestAsk
			var finalLossMicros uint64
			var finalNeededShares uint64
			var finalCost uint64
			var finalDeltaSpent uint64
			var finalDeltaShares uint64
			var finalOk bool
			var finalErrMsg string
			for attempt := 1; attempt <= stopLossMaxRetries; attempt++ {
				bestAskA, okAskA := bestAskFromBook(bookA)
				if okAskA {
					st.a.lastAskPriceMicros = bestAskA.PriceMicros
					st.a.lastAskSharesMicros = bestAskA.SharesMicros
				} else if st.a.lastAskPriceMicros > 0 && st.a.lastAskSharesMicros > 0 {
					bestAskA = arbitrage.BestAsk{PriceMicros: st.a.lastAskPriceMicros, SharesMicros: st.a.lastAskSharesMicros}
					okAskA = true
				}
				bestAskB, okAskB := bestAskFromBook(bookB)
				if okAskB {
					st.b.lastAskPriceMicros = bestAskB.PriceMicros
					st.b.lastAskSharesMicros = bestAskB.SharesMicros
				} else if st.b.lastAskPriceMicros > 0 && st.b.lastAskSharesMicros > 0 {
					bestAskB = arbitrage.BestAsk{PriceMicros: st.b.lastAskPriceMicros, SharesMicros: st.b.lastAskSharesMicros}
					okAskB = true
				}
				if !okAskA && !okAskB {
					logRangeStatus(st, "stop-loss waiting (missing asks)")
					return
				}
				roleA, roleB := rangeRoles(bestAskA, bestAskB)
				if roleA != rangeRoleWin && roleB != rangeRoleWin {
					logRangeStatus(st, "stop-loss waiting (no side leading)")
					return
				}

				st.rangeStopLossLastAt = now

				winSide := &st.a
				winSideLabel := "A"
				winBest := bestAskA
				loseSide := &st.b
				loseBid := bestBidB
				loseBidOk := okBidB
				if roleB == rangeRoleWin {
					winSide = &st.b
					winSideLabel = "B"
					winBest = bestAskB
					loseSide = &st.a
					loseBid = bestBidA
					loseBidOk = okBidA
				}

				if !loseBidOk || loseBid == 0 {
					if loseSide.lastBidMicros > 0 {
						loseBid = loseSide.lastBidMicros
						loseBidOk = true
					}
				}
				if winBest.PriceMicros == 0 || winBest.SharesMicros == 0 {
					logRangeStatus(st, "stop-loss waiting (missing win ask)")
					return
				}
				if winBest.PriceMicros >= parsed.rangeStopLossMaxPriceMicros {
					// Too expensive to add to the winner; stop future attempts.
					st.rangeStopLossDisabled = true
					logRangeStatus(st, "stop-loss disabled (win price >= 0.96)")
					return
				}

				profitTargetMicros := mulDivCeilU64(invested, uint64(parsed.rangeProfitTargetBps), 10_000)
				targetTotalMicros := invested + profitTargetMicros
				if targetTotalMicros < invested {
					targetTotalMicros = math.MaxUint64
				}
				if winSide.heldSharesMicros >= targetTotalMicros {
					finalPnlMicros := winSide.heldSharesMicros - invested
					logRangeStatus(st, fmt.Sprintf("stop-loss skipped (win=%s price=%s resolution pnl %s >= target %s)", winSideLabel, formatMicros(winBest.PriceMicros), formatMicros(finalPnlMicros), formatMicros(profitTargetMicros)))
					return
				}

				loseCost := loseSide.spentMicros
				loseValue := loseCost
				if loseBidOk && loseBid > 0 && loseSide.heldSharesMicros > 0 {
					loseValue = mulDivU64(loseSide.heldSharesMicros, loseBid, microsScale)
				}
				lossMicros := uint64(0)
				if loseCost > loseValue {
					lossMicros = loseCost - loseValue
				}
				requiredProfitMicros := lossMicros + profitTargetMicros
				profitPerShareMicros := microsScale - winBest.PriceMicros
				if requiredProfitMicros == 0 || profitPerShareMicros == 0 {
					return
				}
				neededShares := mulDivCeilU64(requiredProfitMicros, microsScale, profitPerShareMicros)
				if neededShares == 0 {
					return
				}
				if neededShares > winBest.SharesMicros {
					logRangeStatus(st, "stop-loss waiting (insufficient win-side shares)")
					return
				}
				remainingBudget := parsed.rangeStopLossBudgetMicros
				if winSide.rangeStopLossSpentMicros >= remainingBudget {
					if rangeStopLossBudgetsExhausted(st, parsed.rangeStopLossBudgetMicros) {
						st.rangeStopLossDisabled = true
					}
					logRangeStatus(st, fmt.Sprintf("stop-loss budget exhausted (win=%s)", winSideLabel))
					return
				}
				remainingBudget -= winSide.rangeStopLossSpentMicros
				if remainingBudget < minBuyMicros {
					if rangeStopLossBudgetsExhausted(st, parsed.rangeStopLossBudgetMicros) {
						st.rangeStopLossDisabled = true
					}
					logRangeStatus(st, fmt.Sprintf("stop-loss budget exhausted (win=%s)", winSideLabel))
					return
				}
				maxSharesBudget := arbitrage.MaxSharesFromCapMicros(remainingBudget, winBest.PriceMicros)
				if maxSharesBudget == 0 {
					logRangeStatus(st, "stop-loss waiting (budget too small)")
					return
				}
				if neededShares > maxSharesBudget {
					neededShares = maxSharesBudget
				}
				rem := remainingCap(winSide.capMicros, winSide.spentMicros)
				maxSharesCap := arbitrage.MaxSharesFromCapMicros(rem, winBest.PriceMicros)
				if maxSharesCap < neededShares {
					logRangeStatus(st, "stop-loss waiting (cap too small)")
					return
				}
				cost := arbitrage.CostMicrosForShares(neededShares, winBest.PriceMicros)
				if cost < minBuyMicros {
					logRangeStatus(st, "stop-loss add below min; clamping to $1.01")
					log.Printf(
						"[range] stop-loss add below min: price=%s shares=%s cost=%s min=%s",
						formatMicros(winBest.PriceMicros),
						formatShares(neededShares),
						formatMicros(cost),
						formatMicros(stopLossMinBuyMicros),
					)

					minShares := mulDivCeilU64(stopLossMinBuyMicros, microsScale, winBest.PriceMicros)
					if minShares == 0 {
						return
					}
					neededShares = minShares
					if neededShares > winBest.SharesMicros {
						logRangeStatus(st, "stop-loss waiting (insufficient win-side shares)")
						return
					}
					cost = arbitrage.CostMicrosForShares(neededShares, winBest.PriceMicros)
					if cost < stopLossMinBuyMicros {
						cost = stopLossMinBuyMicros
					}
					if cost > remainingBudget {
						logRangeStatus(st, "stop-loss waiting (budget too small)")
						return
					}
					if cost > rem {
						logRangeStatus(st, "stop-loss waiting (cap too small)")
						return
					}
				}

				finalWinSide = winSide
				finalWinBest = winBest
				finalLossMicros = lossMicros
				finalNeededShares = neededShares
				finalCost = cost

				ok := true
				var errMsg string
				preSpent := winSide.spentMicros
				preShares := winSide.heldSharesMicros
				cooldownTriggered := false
				if parsed.enableTrading {
					var buyErr error
					if parsed.rangeStopLossMarket {
						_, buyErr = executeMarketBuy(ctx, clobClient, winSide, cost, parsed.orderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, "stop_loss_add")
					} else {
						_, buyErr = executeExactBuy(ctx, clobClient, winSide, cost, neededShares, parsed.orderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, "stop_loss_add")
					}
					if buyErr != nil {
						ok = false
						errMsg = buyErr.Error()
						if isNotEnoughBalanceOrAllowance(errMsg) && parsed.balanceAllowanceCooldown > 0 {
							st.buyCooldownUntil = time.Now().Add(parsed.balanceAllowanceCooldown)
							log.Printf("[warn] pausing buys for %s due to balance/allowance (until %s)", parsed.balanceAllowanceCooldown, st.buyCooldownUntil.Format(time.RFC3339))
							logBalanceAllowanceSnapshot(ctx, clobClient, st, cost)
							cooldownTriggered = true
						}
					}
				} else {
					applyDryRunBuyLot(winSide, winBest.PriceMicros, neededShares, cost)
				}
				deltaSpent := winSide.spentMicros - preSpent
				deltaShares := winSide.heldSharesMicros - preShares

				finalDeltaSpent = deltaSpent
				finalDeltaShares = deltaShares
				finalOk = ok || !parsed.enableTrading
				finalErrMsg = errMsg

				if ok || !parsed.enableTrading {
					if deltaSpent > 0 {
						winSide.rangeStopLossSpentMicros += deltaSpent
						if winSide.rangeStopLossSpentMicros >= parsed.rangeStopLossBudgetMicros {
							if rangeStopLossBudgetsExhausted(st, parsed.rangeStopLossBudgetMicros) {
								st.rangeStopLossDisabled = true
							}
							logRangeStatus(st, fmt.Sprintf("stop-loss budget exhausted (win=%s)", winSideLabel))
						}
					}
				}

				retryMsg := ""
				if ok || !parsed.enableTrading {
					if deltaShares == 0 && parsed.enableTrading {
						if attempt < stopLossMaxRetries {
							retryMsg = fmt.Sprintf("stop-loss retry (no fill %d/%d)", attempt, stopLossMaxRetries)
						} else {
							finalOk = false
							finalErrMsg = fmt.Sprintf("no fill after %d attempts", stopLossMaxRetries)
							logRangeStatus(st, "stop-loss add failed (no fill)")
						}
					} else if deltaShares == 0 {
						logRangeStatus(st, "stop-loss add posted (no fill)")
					} else {
						logRangeStatus(st, "stop-loss add filled")
					}
				} else {
					if !cooldownTriggered && parsed.enableTrading && attempt < stopLossMaxRetries {
						retryMsg = fmt.Sprintf("stop-loss retry (error %d/%d)", attempt, stopLossMaxRetries)
					} else {
						logRangeStatus(st, "stop-loss add failed")
					}
				}

				if retryMsg != "" {
					logRangeStatus(st, retryMsg)
					continue
				}

				if finalWinSide == nil {
					return
				}
				if finalDeltaShares == 0 {
					log.Printf("[stop-loss] add token=%s price=%s req_shares=%s req_cost=%s loss=%s target=%.2f%% invested=%s (no fill)", finalWinSide.tokenID, formatMicros(finalWinBest.PriceMicros), formatShares(finalNeededShares), formatMicros(finalCost), formatMicros(finalLossMicros), targetPct, formatMicros(invested))
				} else {
					log.Printf("[stop-loss] add token=%s price=%s filled_shares=%s filled_cost=%s loss=%s target=%.2f%% invested=%s", finalWinSide.tokenID, formatMicros(finalWinBest.PriceMicros), formatShares(finalDeltaShares), formatMicros(finalDeltaSpent), formatMicros(finalLossMicros), targetPct, formatMicros(invested))
				}

				logArbEvent(tradeLog, arbLogEvent{
					TsMs:          time.Now().UnixMilli(),
					Event:         "buy_stop_loss",
					Mode:          arbMode(parsed.enableTrading),
					Source:        parsed.source,
					EventSlug:     parsed.eventSlug,
					WindowSlug:    windowSlug,
					TokenID:       finalWinSide.tokenID,
					MakerMicros:   finalCost,
					SharesMicros:  finalNeededShares,
					Price:         formatMicros(finalWinBest.PriceMicros),
					OrderType:     string(parsed.orderType),
					Reason:        "stop_loss_add",
					EnableTrading: parsed.enableTrading,
					Ok:            finalOk,
					Err:           finalErrMsg,
					UptimeMs:      uptimeMs(runStartedAt),
				})
				break
			}
		}
	}

	// If one side is flat and the other isn't, we're exposed. If no hedge is
	// currently available and PnL meets the threshold, sell the remaining side.
	emergency := (sellableA == 0) != (sellableB == 0)
	if emergency {
		heldSide := &st.a
		heldBook := bookA
		heldShares := sellableA
		heldLabel := "A"
		if sellableA == 0 {
			heldSide = &st.b
			heldBook = bookB
			heldShares = sellableB
			heldLabel = "B"
		}
		if heldShares > 0 && heldShares < minSellSharesMicros {
			return
		}

		bidsHeld, _, err := bidsLevelsFromBook(heldBook)
		if err != nil {
			log.Printf("[warn] token%s bids: %v", heldLabel, err)
			return
		}

		_, ok := sellValueMicrosFromBids(bidsHeld, heldShares)
		if !ok {
			return
		}

		invested := heldSide.spentMicros
		if invested == 0 {
			return
		}
		if missingBid && !exitNow {
			return
		}
		pnlBps := pnlBpsFromMicros(invested, positionValueMicros)
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:                time.Now().UnixMilli(),
			Event:               "sell_eval_unhedged",
			Mode:                arbMode(parsed.enableTrading),
			Source:              parsed.source,
			EventSlug:           parsed.eventSlug,
			WindowSlug:          windowSlug,
			TokenID:             heldSide.tokenID,
			TotalInvestedMicros: invested,
			MarkGrossMicros:     positionValueMicros,
			MarkNetMicros:       positionValueMicros,
			PnLBps:              pnlBps,
			MinProfitBps:        minProfitBps,
			TimeRemainingMs:     remainingMs,
			EnableTrading:       parsed.enableTrading,
			UptimeMs:            uptimeMs(runStartedAt),
		})

		if !exitNow && pnlBps < int64(minProfitBps) {
			return
		}

		if !parsed.enableTrading {
			log.Printf(
				"[dry] SELL unhedged: pnl=%.4f%% (min=%.4f%%) invested=%s value=%s shares=%s side=%s",
				float64(pnlBps)/100.0,
				float64(minProfitBps)/100.0,
				formatMicros(invested),
				formatMicros(positionValueMicros),
				formatShares(heldShares),
				heldLabel,
			)

			clearSideAfterSell(heldSide)
			return
		}

		if !clobClient.HasApiCreds() {
			log.Printf("[warn] sell unhedged triggered but api creds not configured")
			return
		}

		filledShares, _, err := executeMarketSell(ctx, clobClient, heldSide, heldShares, parsed.sellSlippageBps, sellOrderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, "sell_unhedged")
		if err != nil && sellOrderType == clob.OrderTypeFOK {
			filledShares, _, err = executeMarketSell(ctx, clobClient, heldSide, heldShares, parsed.sellSlippageBps, clob.OrderTypeFAK, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, "sell_unhedged")
		}
		if err != nil || filledShares == 0 {
			return
		}

		if heldSide == &st.a {
			applySellFills(st, filledShares, 0)
		} else {
			applySellFills(st, 0, filledShares)
		}
		clearRangeDustIfAny(st)
		return
	}

	bidsA, _, err := bidsLevelsFromBook(bookA)
	if err != nil {
		log.Printf("[warn] tokenA bids: %v", err)
		return
	}
	bidsB, _, err := bidsLevelsFromBook(bookB)
	if err != nil {
		log.Printf("[warn] tokenB bids: %v", err)
		return
	}

	grossA, okA := sellValueMicrosFromBids(bidsA, sellableA)
	grossB, okB := sellValueMicrosFromBids(bidsB, sellableB)
	if !okA || !okB {
		return
	}

	markGross := grossA + grossB
	markNet := markGross

	invested = st.a.spentMicros + st.b.spentMicros
	if invested == 0 {
		return
	}
	if missingBid && !exitNow {
		return
	}
	pnlBps = pnlBpsFromMicros(invested, positionValueMicros)
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:                time.Now().UnixMilli(),
		Event:               "sell_eval",
		Mode:                arbMode(parsed.enableTrading),
		Source:              parsed.source,
		EventSlug:           parsed.eventSlug,
		WindowSlug:          windowSlug,
		TokenA:              st.a.tokenID,
		TokenB:              st.b.tokenID,
		TotalInvestedMicros: invested,
		MarkGrossMicros:     positionValueMicros,
		MarkNetMicros:       positionValueMicros,
		PnLBps:              pnlBps,
		MinProfitBps:        minProfitBps,
		TimeRemainingMs:     remainingMs,
		EnableTrading:       parsed.enableTrading,
		UptimeMs:            uptimeMs(runStartedAt),
	})

	if !exitNow && pnlBps < int64(minProfitBps) {
		return
	}

	// Trigger: exit both sides.
	st.rangeExitActive = true
	sellReason := "profit_target_hit"
	if forceSell {
		sellReason = "force_sell_time"
	} else if exitNow && pnlBps < int64(minProfitBps) {
		sellReason = "exit_active"
	}

	if !parsed.enableTrading {
		log.Printf(
			"[dry] SELL trigger: pnl=%.4f%% (min=%.4f%%) invested=%s value=%s sharesA=%s sharesB=%s",
			float64(pnlBps)/100.0,
			float64(minProfitBps)/100.0,
			formatMicros(invested),
			formatMicros(positionValueMicros),
			formatShares(sharesA),
			formatShares(sharesB),
		)

		clearSideAfterSell(&st.a)
		clearSideAfterSell(&st.b)
		return
	}

	if !clobClient.HasApiCreds() {
		log.Printf("[warn] sell triggered but api creds not configured")
		return
	}

	type sellResult struct {
		side         string
		filledShares uint64
		err          error
	}
	resCh := make(chan sellResult, 2)
	expected := 0
	if sellableA > 0 {
		expected++
		go func() {
			filled, _, err := executeMarketSell(ctx, clobClient, &st.a, sellableA, parsed.sellSlippageBps, sellOrderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, sellReason)
			resCh <- sellResult{side: "A", filledShares: filled, err: err}
		}()
	}
	if sellableB > 0 {
		expected++
		go func() {
			filled, _, err := executeMarketSell(ctx, clobClient, &st.b, sellableB, parsed.sellSlippageBps, sellOrderType, parsed.useServerTime, saltGen, tradeLog, runStartedAt, windowSlug, sellReason)
			resCh <- sellResult{side: "B", filledShares: filled, err: err}
		}()
	}

	var errA error
	var errB error
	var filledA uint64
	var filledB uint64
	for i := 0; i < expected; i++ {
		r := <-resCh
		if r.side == "A" {
			errA = r.err
			filledA = r.filledShares
		} else {
			errB = r.err
			filledB = r.filledShares
		}
	}

	if filledA > 0 || filledB > 0 {
		applySellFills(st, filledA, filledB)
	}
	clearRangeDustIfAny(st)

	if errA != nil || errB != nil {
		log.Printf("[warn] sell attempt incomplete: errA=%v errB=%v", errA, errB)
		return
	}
	if st.a.heldSharesMicros > 0 || st.b.heldSharesMicros > 0 {
		log.Printf("[warn] sell attempt partial: remaining A=%s B=%s", formatShares(st.a.heldSharesMicros), formatShares(st.b.heldSharesMicros))
		return
	}

	log.Printf("[sell] ok: exited both sides (A=%s, B=%s)", formatShares(sharesA), formatShares(sharesB))
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:                time.Now().UnixMilli(),
		Event:               "sell",
		Mode:                "live",
		Source:              parsed.source,
		EventSlug:           parsed.eventSlug,
		WindowSlug:          windowSlug,
		TokenA:              st.a.tokenID,
		TokenB:              st.b.tokenID,
		TotalInvestedMicros: invested,
		MarkGrossMicros:     markGross,
		MarkNetMicros:       markNet,
		PnLBps:              pnlBps,
		MinProfitBps:        minProfitBps,
		TimeRemainingMs:     remainingMs,
		Reason:              sellReason,
		EnableTrading:       true,
		UptimeMs:            uptimeMs(runStartedAt),
	})

}

func bidsLevelsFromBook(book *clob.OrderBookSummary) ([]arbitrage.Level, uint64, error) {
	if book == nil {
		return nil, 0, fmt.Errorf("nil orderbook")
	}
	minOrder, err := arbitrage.ParseMicros(book.MinOrder)
	if err != nil {
		return nil, 0, fmt.Errorf("parse min_order_size %q: %w", book.MinOrder, err)
	}
	minSize := minSellSharesMicros
	if minOrder > minSize {
		minSize = minOrder
	}

	levels := make([]arbitrage.Level, 0, len(book.Bids))
	for _, b := range book.Bids {
		price, err := arbitrage.ParseMicros(b.Price)
		if err != nil {
			continue
		}
		size, err := arbitrage.ParseMicros(b.Size)
		if err != nil {
			continue
		}
		if size < minSize {
			continue
		}
		levels = append(levels, arbitrage.Level{PriceMicros: price, SharesMicros: size})
	}
	levels = arbitrage.NormalizeLevels(levels)
	// Descending by price for sells.
	for i, j := 0, len(levels)-1; i < j; i, j = i+1, j-1 {
		levels[i], levels[j] = levels[j], levels[i]
	}
	return levels, minOrder, nil
}

func sellValueMicrosFromBids(bids []arbitrage.Level, sharesMicros uint64) (grossMicros uint64, ok bool) {
	if sharesMicros == 0 {
		return 0, true
	}
	if len(bids) == 0 {
		return 0, false
	}

	rem := sharesMicros
	for i := range bids {
		if rem == 0 {
			break
		}
		l := bids[i]
		if l.PriceMicros == 0 || l.SharesMicros == 0 {
			continue
		}
		take := l.SharesMicros
		if take > rem {
			take = rem
		}

		grossMicros += mulDivU64(take, l.PriceMicros, microsScale)

		rem -= take
	}

	if rem != 0 {
		return grossMicros, false
	}
	return grossMicros, true
}

func pnlBpsFromMicros(investedMicros, markNetMicros uint64) int64 {
	if investedMicros == 0 {
		return 0
	}
	if markNetMicros >= investedMicros {
		profit := markNetMicros - investedMicros
		return int64(mulDivU64(profit, 10_000, investedMicros))
	}
	loss := investedMicros - markNetMicros
	return -int64(mulDivU64(loss, 10_000, investedMicros))
}

func clearSideAfterSell(s *sideState) {
	if s == nil {
		return
	}
	s.spentMicros = 0
	s.heldSharesMicros = 0
	s.inv = arbitrage.Inventory{}
	s.rangeRoleLock = rangeRoleNone
	s.rangeStopLossSpentMicros = 0
	s.lastBidMicros = 0
	s.lastAskPriceMicros = 0
	s.lastAskSharesMicros = 0
}

func clearRangeDustIfAny(st *botState) {
	if st == nil {
		return
	}
	// Range strategy treats sub-min sell leftovers as dust and resets to flat.
	if st.a.heldSharesMicros > 0 && st.a.heldSharesMicros < minSellSharesMicros {
		clearSideAfterSell(&st.a)
	}
	if st.b.heldSharesMicros > 0 && st.b.heldSharesMicros < minSellSharesMicros {
		clearSideAfterSell(&st.b)
	}
	if st.a.heldSharesMicros == 0 && st.b.heldSharesMicros == 0 {
		st.rangeEntryStartedAt = time.Time{}
		st.rangeStopLossLastAt = time.Time{}
		st.rangeStopLossDisabled = false
		st.a.rangeStopLossSpentMicros = 0
		st.b.rangeStopLossSpentMicros = 0
		st.rangeStopLossTick = false
		st.rangeExitActive = false
		st.rangeWinBoughtSide = rangeSideNone
		st.rangeWinBought = false
		st.rangeLoseBought = false
		st.rangeStatus = ""
		st.rangeStatusLastAt = time.Time{}
	}
	clearEqualStateIfFlat(st)
}

func formatTimeOrZero(t time.Time) string {
	if t.IsZero() {
		return "zero"
	}
	return t.Format(time.RFC3339)
}

func logRangeStatus(st *botState, msg string) {
	if st == nil || msg == "" {
		return
	}
	now := time.Now()
	const minInterval = 15 * time.Second
	if st.rangeStatus == msg && !st.rangeStatusLastAt.IsZero() && now.Sub(st.rangeStatusLastAt) < minInterval {
		return
	}
	st.rangeStatus = msg
	st.rangeStatusLastAt = now
	log.Printf("[range] status: %s", msg)
}

func applySellFills(st *botState, filledA, filledB uint64) {
	if st == nil {
		return
	}
	if filledA == 0 && filledB == 0 {
		return
	}
	if filledA > 0 {
		heldA := st.a.inv.TotalSharesMicros()
		if filledA > heldA {
			filledA = heldA
		}
		if filledA > 0 {
			st.a.inv.RemoveSharesMicros(filledA)
		}
	}

	if filledB > 0 {
		heldB := st.b.inv.TotalSharesMicros()
		if filledB > heldB {
			filledB = heldB
		}
		if filledB > 0 {
			st.b.inv.RemoveSharesMicros(filledB)
		}
	}

	st.a.spentMicros = st.a.inv.TotalCostMicros()
	st.b.spentMicros = st.b.inv.TotalCostMicros()
	st.a.heldSharesMicros = st.a.inv.TotalSharesMicros()
	st.b.heldSharesMicros = st.b.inv.TotalSharesMicros()

	resetRangeRoleIfFlat(&st.a)
	resetRangeRoleIfFlat(&st.b)
}

func mulDivU64(a, b, div uint64) uint64 {
	if div == 0 {
		panic("mulDivU64: div=0")
	}
	hi, lo := bits.Mul64(a, b)
	if hi == 0 {
		return lo / div
	}
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
	return ^uint64(0)
}

func mulDivCeilU64(a, b, div uint64) uint64 {
	if div == 0 {
		panic("mulDivCeilU64: div=0")
	}
	var x big.Int
	x.SetUint64(a)
	x.Mul(&x, new(big.Int).SetUint64(b))
	if div > 1 {
		x.Add(&x, new(big.Int).SetUint64(div-1))
	}
	x.Div(&x, new(big.Int).SetUint64(div))
	if x.IsUint64() {
		return x.Uint64()
	}
	return ^uint64(0)
}

func parseProfitThresholdBps(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	ls := strings.ToLower(s)
	if strings.HasSuffix(ls, "bps") {
		ls = strings.TrimSpace(ls[:len(ls)-3])
		v, err := strconv.Atoi(ls)
		if err != nil {
			return 0, err
		}
		return v, nil
	}

	if strings.HasSuffix(ls, "%") {
		ls = strings.TrimSpace(ls[:len(ls)-1])
	}

	pct, err := strconv.ParseFloat(ls, 64)
	if err != nil {
		iv, err2 := strconv.Atoi(ls)
		if err2 != nil {
			return 0, err
		}
		return iv * 100, nil
	}
	if pct < 0 {
		return 0, fmt.Errorf("negative")
	}
	return int(math.Round(pct * 100)), nil
}

func parseArgs() (args, error) {
	var tokenAFlag string
	var tokenBFlag string
	var eventSlugsFlag string
	var eventSlugsFileFlag string
	var gammaURLFlag string
	var eventTZFlag string

	var capAFlag string
	var capBFlag string
	var capSplitBalanceFlag bool
	var rangeEntryUSDFlag string
	var startDelayFlag time.Duration
	var winRangeMinFlag string
	var winRangeMaxFlag string
	var loseRangeMinFlag string
	var loseRangeMaxFlag string
	var rangeStopLossAfterFlag time.Duration
	var rangeStopLossBudgetFlag string
	var rangeStopLossMaxPriceFlag string
	var rangeStopLossMarketFlag bool
	var pollFlag time.Duration

	var sourceFlag string
	var rtdsURLFlag string
	var rtdsPingFlag time.Duration
	var rtdsMinIntervalFlag time.Duration

	var clobHostFlag string
	var privateKeyFlag string
	var funderFlag string

	signatureTypeDefault := 0
	if env := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_SIGNATURE_TYPE"), os.Getenv("SIGNATURE_TYPE"))); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid signature type env %q: %w", env, err)
		}
		signatureTypeDefault = v
	}
	var signatureTypeFlag int

	var apiKeyFlag string
	var apiSecretFlag string
	var apiPassphraseFlag string
	var apiNonceFlag uint64
	var useServerTimeFlag bool

	var orderTypeFlag string
	var buyOrderTypeFlag string
	var enableTradingFlag bool

	buyOrderTypeDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("BUY_ORDER_TYPE"), "FAK"))

	enableTradingDefault := false
	if env := strings.TrimSpace(os.Getenv("ENABLE_TRADING")); env != "" {
		v, err := strconv.ParseBool(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ENABLE_TRADING %q: %w", env, err)
		}
		enableTradingDefault = v
	}

	var outFlag string

	if v := strings.TrimSpace(os.Getenv("SELL_ENABLE")); v != "" {
		log.Printf("[warn] SELL_ENABLE is deprecated and ignored; sell logic is controlled per-strategy")
	} else if v := strings.TrimSpace(os.Getenv("SELL_ENABLED")); v != "" {
		log.Printf("[warn] SELL_ENABLED is deprecated and ignored; sell logic is controlled per-strategy")
	} else if v := strings.TrimSpace(os.Getenv("ENABLE_SELL")); v != "" {
		log.Printf("[warn] ENABLE_SELL is deprecated and ignored; sell logic is controlled per-strategy")
	}

	sellEveryDefault := 1300 * time.Millisecond
	if env := strings.TrimSpace(os.Getenv("SELL_EVERY")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid SELL_EVERY %q: %w", env, err)
		}
		sellEveryDefault = v
	}
	if sellEveryDefault < 0 {
		return args{}, fmt.Errorf("SELL_EVERY must be >= 0")
	}

	sellLastDefault := 3 * time.Minute
	if env := strings.TrimSpace(os.Getenv("SELL_LAST")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid SELL_LAST %q: %w", env, err)
		}
		sellLastDefault = v
	}
	if sellLastDefault < 0 {
		return args{}, fmt.Errorf("SELL_LAST must be >= 0")
	}

	sellMinEarlyProfitDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("SELL_MIN_PROFIT_EARLY"), "3"))
	sellMinLateProfitDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("SELL_MIN_PROFIT_LATE"), "1"))

	sellForceLastDefault := time.Minute
	if env := strings.TrimSpace(os.Getenv("SELL_FORCE_LAST")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid SELL_FORCE_LAST %q: %w", env, err)
		}
		sellForceLastDefault = v
	}
	if sellForceLastDefault < 0 {
		return args{}, fmt.Errorf("SELL_FORCE_LAST must be >= 0")
	}

	sellDisableBuysInLastDefault := true
	if env := strings.TrimSpace(os.Getenv("SELL_DISABLE_BUYS_IN_LAST")); env != "" {
		v, err := strconv.ParseBool(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid SELL_DISABLE_BUYS_IN_LAST %q: %w", env, err)
		}
		sellDisableBuysInLastDefault = v
	}

	sellOrderTypeDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("SELL_ORDER_TYPE"), "FAK"))
	sellSlippageBpsDefault := 0
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_SELL_SLIPPAGE_BPS")); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_SELL_SLIPPAGE_BPS %q: %w", env, err)
		}
		sellSlippageBpsDefault = v
	}

	var sellEveryFlag time.Duration
	var sellLastFlag time.Duration
	var sellForceLastFlag time.Duration
	var sellMinEarlyProfitFlag string
	var sellMinLateProfitFlag string
	var sellDisableBuysInLastFlag bool
	var sellOrderTypeFlag string
	var sellSlippageBpsFlag int

	nextSlugLastDefault := time.Duration(0)
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_NEXT_SLUG_LAST")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_NEXT_SLUG_LAST %q: %w", env, err)
		}
		nextSlugLastDefault = v
	}
	if nextSlugLastDefault < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_NEXT_SLUG_LAST must be >= 0")
	}
	var nextSlugLastFlag time.Duration

	balAllowanceCooldownDefault := 30 * time.Second
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_BALANCE_ALLOWANCE_COOLDOWN")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_BALANCE_ALLOWANCE_COOLDOWN %q: %w", env, err)
		}
		balAllowanceCooldownDefault = v
	}
	if balAllowanceCooldownDefault < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_BALANCE_ALLOWANCE_COOLDOWN must be >= 0")
	}
	var balAllowanceCooldownFlag time.Duration

	flag.StringVar(&tokenAFlag, "token-a", "", "Token ID A (string)")
	flag.StringVar(&tokenBFlag, "token-b", "", "Token ID B (string)")
	flag.StringVar(&eventSlugsFlag, "event-slug", "", "Rolling event slug stem(s) (comma-separated) (e.g. btc-updown-15m,eth-updown-15m). Also supports hourly stems like bitcoin-up-or-down. Resolves token IDs via Gamma and auto-switches each window")
	flag.StringVar(&eventSlugsFlag, "event-slugs", "", "Rolling event slug stem(s) (comma-separated) (alias for --event-slug)")
	flag.StringVar(&eventSlugsFileFlag, "event-slugs-file", "", "Path to a text file containing rolling event slug stems (one per line; supports # comments). Can be reloaded via SIGHUP.")
	flag.StringVar(&eventSlugsFileFlag, "slugs-file", "", "Path to a text file containing rolling event slug stems (alias for --event-slugs-file)")
	flag.StringVar(&gammaURLFlag, "gamma-url", "", "Gamma API base URL (default https://gamma-api.polymarket.com)")
	flag.StringVar(&eventTZFlag, "event-tz", "America/New_York", "IANA timezone used for windowing (default America/New_York)")

	capSplitBalanceDefault := false
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_CAP_SPLIT_BALANCE")); env != "" {
		v, err := strconv.ParseBool(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_CAP_SPLIT_BALANCE %q: %w", env, err)
		}
		capSplitBalanceDefault = v
	}
	flag.BoolVar(&capSplitBalanceFlag, "cap-split-balance", capSplitBalanceDefault, "Set cap-a and cap-b to 1/2 of on-chain USDC balance (or ARBITRAGE_CAP_SPLIT_BALANCE env; ignored in range-only mode)")

	capADefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_CAP_A"), os.Getenv("CAP_A"), "50"))
	capBDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_CAP_B"), os.Getenv("CAP_B"), "50"))
	flag.StringVar(&capAFlag, "cap-a", capADefault, "Dollar cap for token A (e.g. 50 or 50.0) (or ARBITRAGE_CAP_A/CAP_A env)")
	flag.StringVar(&capBFlag, "cap-b", capBDefault, "Dollar cap for token B (e.g. 50 or 50.0) (or ARBITRAGE_CAP_B/CAP_B env)")
	rangeEntryUSDDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_RANGE_ENTRY_USD"), "3"))
	rangeProfitTargetRaw := strings.TrimSpace(firstNonEmpty(
		os.Getenv("ARBITRAGE_RANGE_PROFIT_TARGET"),
		fmt.Sprintf("%.2f%%", float64(defaultRangeProfitTargetBps)/100.0),
	))
	rangeProfitTargetBps, err := parseProfitThresholdBps(rangeProfitTargetRaw)
	if err != nil {
		return args{}, fmt.Errorf("invalid ARBITRAGE_RANGE_PROFIT_TARGET %q: %w", rangeProfitTargetRaw, err)
	}
	if rangeProfitTargetBps < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_RANGE_PROFIT_TARGET must be >= 0")
	}

	rangeStopLossAfterDefault := 30 * time.Second
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_RANGE_STOP_LOSS_AFTER")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_RANGE_STOP_LOSS_AFTER %q: %w", env, err)
		}
		rangeStopLossAfterDefault = v
	}
	if rangeStopLossAfterDefault < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_RANGE_STOP_LOSS_AFTER must be >= 0")
	}
	rangeStopLossBudgetDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_RANGE_STOP_LOSS_BUDGET"), "10"))
	rangeStopLossMaxPriceDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_RANGE_STOP_LOSS_MAX_PRICE"), "0.96"))
	rangeStopLossMarketDefault := true
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_STOP_LOSS_MARKET")); env != "" {
		v, err := strconv.ParseBool(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_STOP_LOSS_MARKET %q: %w", env, err)
		}
		rangeStopLossMarketDefault = v
	}

	startDelayDefault := 2 * time.Minute
	if env := strings.TrimSpace(os.Getenv("ARBITRAGE_START_DELAY")); env != "" {
		v, err := time.ParseDuration(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ARBITRAGE_START_DELAY %q: %w", env, err)
		}
		startDelayDefault = v
	}
	if startDelayDefault < 0 {
		return args{}, fmt.Errorf("ARBITRAGE_START_DELAY must be >= 0")
	}

	winRangeMinDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WIN_MIN"), "0.52"))
	winRangeMaxDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_WIN_MAX"), "0.85"))
	loseRangeMinDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_LOSE_MIN"), "0.15"))
	loseRangeMaxDefault := strings.TrimSpace(firstNonEmpty(os.Getenv("ARBITRAGE_LOSE_MAX"), "0.46"))
	flag.DurationVar(&startDelayFlag, "start-delay", startDelayDefault, "Delay after window start before enabling buys (rolling slug mode only)")
	flag.StringVar(&winRangeMinFlag, "win-range-min", winRangeMinDefault, "Winning side min price (ARBITRAGE_WIN_MIN env)")
	flag.StringVar(&winRangeMaxFlag, "win-range-max", winRangeMaxDefault, "Winning side max price (ARBITRAGE_WIN_MAX env)")
	flag.StringVar(&loseRangeMinFlag, "lose-range-min", loseRangeMinDefault, "Losing side min price (ARBITRAGE_LOSE_MIN env)")
	flag.StringVar(&loseRangeMaxFlag, "lose-range-max", loseRangeMaxDefault, "Losing side max price (ARBITRAGE_LOSE_MAX env)")
	flag.StringVar(&rangeEntryUSDFlag, "range-entry-usd", rangeEntryUSDDefault, "Total per-entry USD for range strategy (split 2/3 win, 1/3 lose) (ARBITRAGE_RANGE_ENTRY_USD env)")
	flag.DurationVar(&rangeStopLossAfterFlag, "range-stop-loss-after", rangeStopLossAfterDefault, "Range stop-loss check delay after entry (ARBITRAGE_RANGE_STOP_LOSS_AFTER env)")
	flag.StringVar(&rangeStopLossBudgetFlag, "range-stop-loss-budget", rangeStopLossBudgetDefault, "Range stop-loss max add budget per side in USD (ARBITRAGE_RANGE_STOP_LOSS_BUDGET env)")
	flag.StringVar(&rangeStopLossMaxPriceFlag, "range-stop-loss-max-price", rangeStopLossMaxPriceDefault, "Max winning price for range stop-loss add (ARBITRAGE_RANGE_STOP_LOSS_MAX_PRICE env)")
	flag.BoolVar(&rangeStopLossMarketFlag, "range-stop-loss-market", rangeStopLossMarketDefault, "Use market order for range stop-loss adds (ARBITRAGE_STOP_LOSS_MARKET env)")

	flag.DurationVar(&pollFlag, "poll", 500*time.Millisecond, "Polling interval for GET /book (only used with --source=poll)")

	flag.StringVar(&sourceFlag, "source", "rtds", "Orderbook source: rtds (default) or poll")
	flag.StringVar(&rtdsURLFlag, "rtds-url", rtds.DefaultURL, "RTDS WebSocket URL (default wss://ws-live-data.polymarket.com)")
	flag.DurationVar(&rtdsPingFlag, "rtds-ping", rtds.DefaultPingInterval, "RTDS ping interval (sends text message \"ping\")")
	flag.DurationVar(&rtdsMinIntervalFlag, "rtds-min-interval", 500*time.Millisecond, "Minimum time between evaluations when using --source=rtds")

	flag.StringVar(&clobHostFlag, "clob-url", "", "CLOB API base URL (default https://clob.polymarket.com)")
	flag.StringVar(&privateKeyFlag, "private-key", "", "Private key hex (0x...) (or PRIVATE_KEY env)")
	flag.StringVar(&funderFlag, "funder", "", "Funder address (proxy wallet) (default: signer)")
	flag.IntVar(&signatureTypeFlag, "signature-type", signatureTypeDefault, "Signature type: 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE")

	flag.StringVar(&apiKeyFlag, "api-key", "", "CLOB API key (optional; otherwise derived if trading enabled)")
	flag.StringVar(&apiSecretFlag, "api-secret", "", "CLOB API secret (optional)")
	flag.StringVar(&apiPassphraseFlag, "api-passphrase", "", "CLOB API passphrase (optional)")
	flag.Uint64Var(&apiNonceFlag, "api-nonce", 0, "Nonce for API key derive/create")
	flag.BoolVar(&useServerTimeFlag, "use-server-time", true, "Use /time for signed requests")

	flag.StringVar(&orderTypeFlag, "order-type", "FAK", "Order type for buys: FOK or FAK (alias for --buy-order-type)")
	flag.StringVar(&buyOrderTypeFlag, "buy-order-type", buyOrderTypeDefault, "Order type for buys: FOK or FAK")
	flag.BoolVar(&enableTradingFlag, "enable-trading", enableTradingDefault, "Actually place trades (default is dry-run)")

	flag.StringVar(&outFlag, "out", "", "Optional output file path (JSONL; logs trades + pairing/profit events)")
	flag.StringVar(&outFlag, "outfile", "", "Optional output file path (JSONL; logs trades + pairing/profit events) (alias)")

	flag.DurationVar(&sellEveryFlag, "sell-every", sellEveryDefault, "Minimum time between sell evaluations (throttles sell checks)")
	flag.DurationVar(&sellLastFlag, "sell-last", sellLastDefault, "Last window duration where late profit threshold applies (ignored in range-only mode)")
	flag.DurationVar(&sellForceLastFlag, "sell-force-last", sellForceLastDefault, "Force sell when remaining window time is <= this (ignored in range-only mode)")
	flag.StringVar(&sellMinEarlyProfitFlag, "sell-min-profit-early", sellMinEarlyProfitDefault, "Min profit target before last window (percent or bps; ignored in range-only mode)")
	flag.StringVar(&sellMinLateProfitFlag, "sell-min-profit-late", sellMinLateProfitDefault, "Min profit target inside last window (ignored in range-only mode)")
	flag.BoolVar(&sellDisableBuysInLastFlag, "sell-disable-buys-in-last", sellDisableBuysInLastDefault, "Stop placing new buys inside --sell-last window (ignored in range-only mode)")
	flag.StringVar(&sellOrderTypeFlag, "sell-order-type", sellOrderTypeDefault, "Order type for sells: FOK or FAK")
	flag.IntVar(&sellSlippageBpsFlag, "sell-slippage-bps", sellSlippageBpsDefault, "Sell slippage buffer in bps (ARBITRAGE_SELL_SLIPPAGE_BPS env)")
	flag.DurationVar(&balAllowanceCooldownFlag, "balance-allowance-cooldown", balAllowanceCooldownDefault, "Cooldown after 'not enough balance / allowance' failures (ARBITRAGE_BALANCE_ALLOWANCE_COOLDOWN env)")
	flag.DurationVar(&nextSlugLastFlag, "next-slug-last", nextSlugLastDefault, "Start trading the next rolling window once the current window has <= this time remaining (rolling slug mode only) (ARBITRAGE_NEXT_SLUG_LAST env)")

	flag.Parse()

	capAExplicit := false
	capBExplicit := false
	flag.CommandLine.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "cap-a":
			capAExplicit = true
		case "cap-b":
			capBExplicit = true
		}
	})

	eventSlugsFile := strings.TrimSpace(eventSlugsFileFlag)
	if eventSlugsFile == "" {
		eventSlugsFile = strings.TrimSpace(os.Getenv("EVENT_SLUGS_FILE"))
	}
	if eventSlugsFile != "" && strings.TrimSpace(eventSlugsFlag) != "" {
		return args{}, fmt.Errorf("do not combine --event-slug/--event-slugs with --event-slugs-file")
	}

	tokenAFlagVal := strings.TrimSpace(tokenAFlag)
	tokenBFlagVal := strings.TrimSpace(tokenBFlag)

	// If neither slug mode nor explicit token mode is configured, prefer a local
	// ./slugs.txt if it exists (easy editable default).
	if eventSlugsFile == "" && strings.TrimSpace(eventSlugsFlag) == "" && tokenAFlagVal == "" && tokenBFlagVal == "" {
		if p := defaultSlugsFileIfPresent(); p != "" {
			eventSlugsFile = p
		}
	}

	var eventSlugs []string
	if eventSlugsFile != "" {
		slugs, err := readSlugsFile(eventSlugsFile)
		if err != nil {
			return args{}, fmt.Errorf("read slugs file %q: %w", eventSlugsFile, err)
		}
		eventSlugs = slugs
	} else {
		rawSlugs := strings.TrimSpace(eventSlugsFlag)
		if rawSlugs == "" {
			rawSlugs = strings.TrimSpace(os.Getenv("EVENT_SLUGS"))
		}
		if rawSlugs == "" {
			rawSlugs = strings.TrimSpace(os.Getenv("EVENT_SLUG"))
		}
		eventSlugs = parseSlugList(rawSlugs)
	}
	gammaURL := strings.TrimSpace(gammaURLFlag)
	if gammaURL == "" {
		gammaURL = strings.TrimSpace(os.Getenv("GAMMA_URL"))
	}
	eventTZ := strings.TrimSpace(eventTZFlag)
	if eventTZ == "" {
		eventTZ = strings.TrimSpace(os.Getenv("EVENT_TZ"))
	}

	tokenA := tokenAFlagVal
	if tokenA == "" {
		tokenA = strings.TrimSpace(os.Getenv("TOKEN_A"))
	}
	tokenB := tokenBFlagVal
	if tokenB == "" {
		tokenB = strings.TrimSpace(os.Getenv("TOKEN_B"))
	}
	slugMode := len(eventSlugs) > 0 || eventSlugsFile != ""
	if !slugMode {
		return args{}, fmt.Errorf("rolling-slug mode required: set --event-slug/--event-slugs-file or EVENT_SLUGS/EVENT_SLUGS_FILE")
	}
	if tokenA != "" || tokenB != "" {
		return args{}, fmt.Errorf("rolling-slug mode does not accept --token-a/--token-b or TOKEN_A/TOKEN_B")
	}

	capA, err := arbitrage.ParseMicros(capAFlag)
	if err != nil || capA == 0 {
		return args{}, fmt.Errorf("invalid --cap-a %q", capAFlag)
	}
	capB, err := arbitrage.ParseMicros(capBFlag)
	if err != nil || capB == 0 {
		return args{}, fmt.Errorf("invalid --cap-b %q", capBFlag)
	}

	rangeEntryTotalMicros, err := arbitrage.ParseMicros(rangeEntryUSDFlag)
	if err != nil || rangeEntryTotalMicros == 0 {
		return args{}, fmt.Errorf("invalid --range-entry-usd %q", rangeEntryUSDFlag)
	}
	if minRangeEntry := minBuyMicros * rangeEntryParts; rangeEntryTotalMicros < minRangeEntry {
		return args{}, fmt.Errorf("range-entry-usd must be >= %s (min order size split 2/3 win, 1/3 lose)", formatMicros(minRangeEntry))
	}

	rangeStopLossMaxPriceMicros, err := arbitrage.ParseMicros(rangeStopLossMaxPriceFlag)
	if err != nil || rangeStopLossMaxPriceMicros == 0 {
		return args{}, fmt.Errorf("invalid --range-stop-loss-max-price %q", rangeStopLossMaxPriceFlag)
	}
	if rangeStopLossMaxPriceMicros > microsScale {
		return args{}, fmt.Errorf("range-stop-loss-max-price must be <= 1.0")
	}
	rangeStopLossBudgetMicros, err := arbitrage.ParseMicros(rangeStopLossBudgetFlag)
	if err != nil || rangeStopLossBudgetMicros == 0 {
		return args{}, fmt.Errorf("invalid --range-stop-loss-budget %q", rangeStopLossBudgetFlag)
	}
	if rangeStopLossBudgetMicros < minBuyMicros {
		return args{}, fmt.Errorf("range-stop-loss-budget must be >= %s", formatMicros(minBuyMicros))
	}

	winMinMicros, err := arbitrage.ParseMicros(winRangeMinFlag)
	if err != nil {
		return args{}, fmt.Errorf("invalid --win-range-min %q", winRangeMinFlag)
	}
	winMaxMicros, err := arbitrage.ParseMicros(winRangeMaxFlag)
	if err != nil {
		return args{}, fmt.Errorf("invalid --win-range-max %q", winRangeMaxFlag)
	}
	loseMinMicros, err := arbitrage.ParseMicros(loseRangeMinFlag)
	if err != nil {
		return args{}, fmt.Errorf("invalid --lose-range-min %q", loseRangeMinFlag)
	}
	loseMaxMicros, err := arbitrage.ParseMicros(loseRangeMaxFlag)
	if err != nil {
		return args{}, fmt.Errorf("invalid --lose-range-max %q", loseRangeMaxFlag)
	}
	if winMinMicros == 0 || winMaxMicros == 0 || loseMinMicros == 0 || loseMaxMicros == 0 {
		return args{}, fmt.Errorf("range bounds must be > 0")
	}
	if winMinMicros > winMaxMicros {
		return args{}, fmt.Errorf("win-range-min must be <= win-range-max")
	}
	if loseMinMicros > loseMaxMicros {
		return args{}, fmt.Errorf("lose-range-min must be <= lose-range-max")
	}
	if winMaxMicros > microsScale || loseMaxMicros > microsScale {
		return args{}, fmt.Errorf("range max must be <= 1.0")
	}
	if startDelayFlag < 0 {
		return args{}, fmt.Errorf("start-delay must be >= 0")
	}

	if pollFlag <= 0 {
		return args{}, fmt.Errorf("poll interval must be > 0")
	}

	source := strings.ToLower(strings.TrimSpace(sourceFlag))
	if source == "" {
		source = "rtds"
	}
	switch source {
	case "poll", "rtds":
	default:
		return args{}, fmt.Errorf("invalid --source %q (use rtds or poll)", sourceFlag)
	}

	rtdsURL := strings.TrimSpace(rtdsURLFlag)
	if rtdsURL == "" {
		rtdsURL = rtds.DefaultURL
	}
	if rtdsPingFlag <= 0 {
		return args{}, fmt.Errorf("rtds ping interval must be > 0")
	}
	if rtdsMinIntervalFlag <= 0 {
		return args{}, fmt.Errorf("rtds min interval must be > 0")
	}

	pkHex := strings.TrimSpace(privateKeyFlag)
	if pkHex == "" {
		pkHex = firstNonEmpty(os.Getenv("CLOB_PRIVATE_KEY"), os.Getenv("PRIVATE_KEY"))
	}
	if pkHex == "" && enableTradingFlag {
		return args{}, fmt.Errorf("private key required for --enable-trading (set --private-key or PRIVATE_KEY)")
	}

	host := strings.TrimSpace(clobHostFlag)
	if host == "" {
		host = firstNonEmpty(os.Getenv("CLOB_URL"), "https://clob.polymarket.com")
	}

	var funder common.Address
	if strings.TrimSpace(funderFlag) != "" {
		if !common.IsHexAddress(funderFlag) {
			return args{}, fmt.Errorf("invalid funder: %q", funderFlag)
		}
		funder = common.HexToAddress(funderFlag)
	} else if envFunder := firstNonEmpty(os.Getenv("CLOB_FUNDER"), os.Getenv("FUNDER")); envFunder != "" {
		if !common.IsHexAddress(envFunder) {
			return args{}, fmt.Errorf("invalid funder env: %q", envFunder)
		}
		funder = common.HexToAddress(envFunder)
	}

	// Buy order type: prefer --buy-order-type, fall back to --order-type
	buyOrderTypeRaw := strings.TrimSpace(buyOrderTypeFlag)
	if buyOrderTypeRaw == "" || buyOrderTypeRaw == buyOrderTypeDefault {
		// Check if --order-type was explicitly set
		if strings.TrimSpace(orderTypeFlag) != "" && strings.TrimSpace(orderTypeFlag) != buyOrderTypeDefault {
			buyOrderTypeRaw = orderTypeFlag
		}
	}
	if buyOrderTypeRaw == "" {
		buyOrderTypeRaw = buyOrderTypeDefault
	}

	var ot clob.OrderType
	switch strings.ToUpper(buyOrderTypeRaw) {
	case "FOK":
		ot = clob.OrderTypeFOK
	case "FAK":
		ot = clob.OrderTypeFAK
	default:
		return args{}, fmt.Errorf("unsupported buy-order-type %q (use FOK or FAK)", buyOrderTypeRaw)
	}

	apiKey := strings.TrimSpace(apiKeyFlag)
	if apiKey == "" {
		apiKey = firstNonEmpty(os.Getenv("CLOB_API_KEY"), os.Getenv("API_KEY"))
	}
	apiSecret := strings.TrimSpace(apiSecretFlag)
	if apiSecret == "" {
		apiSecret = firstNonEmpty(os.Getenv("CLOB_SECRET"), os.Getenv("SECRET"))
	}
	apiPass := strings.TrimSpace(apiPassphraseFlag)
	if apiPass == "" {
		apiPass = firstNonEmpty(os.Getenv("CLOB_PASSPHRASE"), os.Getenv("PASSPHRASE"))
	}

	outFile := strings.TrimSpace(outFlag)
	if outFile == "" {
		outFile = strings.TrimSpace(os.Getenv("ARBITRAGE_OUT_FILE"))
	}
	if outFile == "" {
		outFile = strings.TrimSpace(os.Getenv("TRADES_OUT_FILE"))
	}
	if outFile == "" {
		outFile = defaultArbOutFile
	}

	sellEvery := sellEveryFlag
	if sellEvery < 0 {
		return args{}, fmt.Errorf("sell-every must be >= 0")
	}

	sellLast := sellLastFlag
	if sellLast < 0 {
		return args{}, fmt.Errorf("sell-last must be >= 0")
	}

	sellForceLast := sellForceLastFlag
	if sellForceLast < 0 {
		return args{}, fmt.Errorf("sell-force-last must be >= 0")
	}

	sellDisableBuysInLast := sellDisableBuysInLastFlag
	if sellLast > 0 || sellForceLast > 0 || sellDisableBuysInLast {
		log.Printf("[cfg] sell-last/sell-force-last/sell-disable-buys-in-last ignored (range-only mode)")
	}
	sellLast = 0
	sellForceLast = 0
	sellDisableBuysInLast = false

	nextSlugLast := nextSlugLastFlag
	if nextSlugLast < 0 {
		return args{}, fmt.Errorf("next-slug-last must be >= 0")
	}
	if nextSlugLast > 0 {
		log.Printf("[cfg] next-slug-last ignored (range-only mode)")
	}
	nextSlugLast = 0

	capSplitBalance := capSplitBalanceFlag
	if capSplitBalance {
		log.Printf("[cfg] cap-split-balance ignored (range-only mode)")
		capSplitBalance = false
	}

	sellMinEarlyStr := strings.TrimSpace(sellMinEarlyProfitFlag)
	if sellMinEarlyStr == "" {
		return args{}, fmt.Errorf("sell-min-profit-early is required")
	}
	sellMinLateStr := strings.TrimSpace(sellMinLateProfitFlag)
	if sellMinLateStr == "" {
		return args{}, fmt.Errorf("sell-min-profit-late is required")
	}
	sellMinEarlyBps, err := parseProfitThresholdBps(sellMinEarlyStr)
	if err != nil {
		return args{}, fmt.Errorf("invalid sell-min-profit-early %q: %w", sellMinEarlyStr, err)
	}
	sellMinLateBps, err := parseProfitThresholdBps(sellMinLateStr)
	if err != nil {
		return args{}, fmt.Errorf("invalid sell-min-profit-late %q: %w", sellMinLateStr, err)
	}
	if sellMinEarlyBps < 0 || sellMinLateBps < 0 {
		return args{}, fmt.Errorf("sell profit thresholds must be >= 0")
	}

	sellOrderTypeRaw := strings.TrimSpace(sellOrderTypeFlag)
	if sellOrderTypeRaw == "" {
		return args{}, fmt.Errorf("sell-order-type is required")
	}
	var sellOT clob.OrderType
	switch strings.ToUpper(sellOrderTypeRaw) {
	case "FOK":
		sellOT = clob.OrderTypeFOK
	case "FAK":
		sellOT = clob.OrderTypeFAK
	default:
		return args{}, fmt.Errorf("unsupported sell-order-type %q (use FOK or FAK)", sellOrderTypeRaw)
	}

	if sellSlippageBpsFlag < 0 {
		return args{}, fmt.Errorf("sell-slippage-bps must be >= 0")
	}
	if sellSlippageBpsFlag >= 10_000 {
		return args{}, fmt.Errorf("sell-slippage-bps must be < 10000")
	}

	return args{
		tokenA:                      tokenA,
		tokenB:                      tokenB,
		eventSlug:                   "",
		eventSlugs:                  eventSlugs,
		eventSlugsFile:              eventSlugsFile,
		gammaURL:                    gammaURL,
		eventTZ:                     eventTZ,
		capAMicros:                  capA,
		capBMicros:                  capB,
		capSplitBalance:             capSplitBalance,
		capAExplicit:                capAExplicit,
		capBExplicit:                capBExplicit,
		startDelay:                  startDelayFlag,
		winRangeMinMicros:           winMinMicros,
		winRangeMaxMicros:           winMaxMicros,
		loseRangeMinMicros:          loseMinMicros,
		loseRangeMaxMicros:          loseMaxMicros,
		rangeEntryTotalMicros:       rangeEntryTotalMicros,
		rangeProfitTargetBps:        int64(rangeProfitTargetBps),
		rangeStopLossAfter:          rangeStopLossAfterFlag,
		rangeStopLossBudgetMicros:   rangeStopLossBudgetMicros,
		rangeStopLossMaxPriceMicros: rangeStopLossMaxPriceMicros,
		rangeStopLossMarket:         rangeStopLossMarketFlag,
		pollInterval:                pollFlag,
		clobHost:                    host,
		privateKeyHex:               pkHex,
		funder:                      funder,
		signatureType:               signatureTypeFlag,
		apiKey:                      apiKey,
		apiSecret:                   apiSecret,
		apiPassphrase:               apiPass,
		apiNonce:                    apiNonceFlag,
		useServerTime:               useServerTimeFlag,
		orderType:                   ot,
		enableTrading:               enableTradingFlag,
		enableSell:                  true,
		source:                      source,
		rtdsURL:                     rtdsURL,
		rtdsPingInterval:            rtdsPingFlag,
		rtdsMinInterval:             rtdsMinIntervalFlag,
		outFile:                     outFile,
		sellEvery:                   sellEvery,
		sellLast:                    sellLast,
		sellForceLast:               sellForceLast,
		sellMinEarlyProfitBps:       sellMinEarlyBps,
		sellMinLateProfitBps:        sellMinLateBps,
		sellDisableBuysInLast:       sellDisableBuysInLast,
		sellOrderType:               sellOT,
		sellSlippageBps:             sellSlippageBpsFlag,
		nextSlugLast:                nextSlugLast,
		balanceAllowanceCooldown:    balAllowanceCooldownFlag,
	}, nil
}

func fetchBooks(ctx context.Context, c *clob.Client, tokenA, tokenB string) (*clob.OrderBookSummary, *clob.OrderBookSummary, error) {
	if c == nil {
		return nil, nil, fmt.Errorf("clob client nil")
	}
	var (
		bookA *clob.OrderBookSummary
		bookB *clob.OrderBookSummary
		errA  error
		errB  error
	)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		bookA, errA = c.GetOrderBook(ctx, tokenA)
	}()
	go func() {
		defer wg.Done()
		bookB, errB = c.GetOrderBook(ctx, tokenB)
	}()
	wg.Wait()
	if errA != nil {
		return nil, nil, errA
	}
	if errB != nil {
		return nil, nil, errB
	}
	return bookA, bookB, nil
}

func asksLevelsFromBook(book *clob.OrderBookSummary) ([]arbitrage.Level, uint64, error) {
	if book == nil {
		return nil, 0, fmt.Errorf("nil orderbook")
	}
	minOrder, err := arbitrage.ParseMicros(book.MinOrder)
	if err != nil {
		return nil, 0, fmt.Errorf("parse min_order_size %q: %w", book.MinOrder, err)
	}

	levels := make([]arbitrage.Level, 0, len(book.Asks))
	for _, a := range book.Asks {
		price, err := arbitrage.ParseMicros(a.Price)
		if err != nil {
			continue
		}
		size, err := arbitrage.ParseMicros(a.Size)
		if err != nil {
			continue
		}
		levels = append(levels, arbitrage.Level{PriceMicros: price, SharesMicros: size})
	}
	return arbitrage.NormalizeLevels(levels), minOrder, nil
}

func bestAskFromLevels(levels []arbitrage.Level) arbitrage.BestAsk {
	if len(levels) == 0 {
		return arbitrage.BestAsk{}
	}
	return arbitrage.BestAsk{PriceMicros: levels[0].PriceMicros, SharesMicros: levels[0].SharesMicros}
}

func remainingCap(capMicros, spentMicros uint64) uint64 {
	if spentMicros >= capMicros {
		return 0
	}
	return capMicros - spentMicros
}

func remainingBudget(budgetMicros, spentMicros uint64) uint64 {
	if spentMicros >= budgetMicros {
		return 0
	}
	return budgetMicros - spentMicros
}

func rangeStopLossBudgetRemaining(budgetMicros, spentMicros uint64) uint64 {
	if spentMicros >= budgetMicros {
		return 0
	}
	return budgetMicros - spentMicros
}

func rangeStopLossBudgetsExhausted(st *botState, budgetMicros uint64) bool {
	if st == nil || budgetMicros == 0 {
		return true
	}
	return rangeStopLossBudgetRemaining(budgetMicros, st.a.rangeStopLossSpentMicros) < minBuyMicros &&
		rangeStopLossBudgetRemaining(budgetMicros, st.b.rangeStopLossSpentMicros) < minBuyMicros
}

func applyDryRunBuyLot(s *sideState, priceMicros, sharesMicros, costMicros uint64) {
	if s == nil || priceMicros == 0 || sharesMicros == 0 || costMicros == 0 {
		return
	}
	rem := remainingCap(s.capMicros, s.spentMicros)
	if costMicros > rem {
		return
	}
	if costMicros < minBuyMicros {
		if rem < minBuyMicros {
			return
		}
		costMicros = minBuyMicros
	}
	s.spentMicros += costMicros
	s.heldSharesMicros += sharesMicros
	s.totalBoughtMicros += costMicros
	s.totalBoughtSharesMicros += sharesMicros
	s.inv.AddLot(priceMicros, sharesMicros)
}

type fillLot struct {
	PriceMicros  uint64
	SharesMicros uint64
}

const (
	fillReconAttempts = 4
	fillReconDelay    = 150 * time.Millisecond
)

func orderIDFromResp(resp map[string]any) string {
	if resp == nil {
		return ""
	}
	if v, ok := resp["orderId"]; ok {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	if v, ok := resp["orderID"]; ok {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func boolFromAny(v any) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		s := strings.TrimSpace(strings.ToLower(t))
		if s == "" {
			return false, false
		}
		switch s {
		case "true", "t", "1", "yes", "y":
			return true, true
		case "false", "f", "0", "no", "n":
			return false, true
		}
	case float64:
		return t != 0, true
	case float32:
		return t != 0, true
	case int:
		return t != 0, true
	case int8:
		return t != 0, true
	case int16:
		return t != 0, true
	case int32:
		return t != 0, true
	case int64:
		return t != 0, true
	case uint:
		return t != 0, true
	case uint8:
		return t != 0, true
	case uint16:
		return t != 0, true
	case uint32:
		return t != 0, true
	case uint64:
		return t != 0, true
	default:
		return false, false
	}
	return false, false
}

func microsFromAny(v any) (uint64, bool) {
	switch t := v.(type) {
	case string:
		if t == "" {
			return 0, false
		}
		m, err := arbitrage.ParseMicros(t)
		if err != nil {
			return 0, false
		}
		return m, true
	case float64:
		s := strconv.FormatFloat(t, 'f', -1, 64)
		m, err := arbitrage.ParseMicros(s)
		if err != nil {
			return 0, false
		}
		return m, true
	case float32:
		s := strconv.FormatFloat(float64(t), 'f', -1, 32)
		m, err := arbitrage.ParseMicros(s)
		if err != nil {
			return 0, false
		}
		return m, true
	case int:
		return microsFromAny(strconv.FormatInt(int64(t), 10))
	case int8:
		return microsFromAny(strconv.FormatInt(int64(t), 10))
	case int16:
		return microsFromAny(strconv.FormatInt(int64(t), 10))
	case int32:
		return microsFromAny(strconv.FormatInt(int64(t), 10))
	case int64:
		return microsFromAny(strconv.FormatInt(t, 10))
	case uint:
		return microsFromAny(strconv.FormatUint(uint64(t), 10))
	case uint8:
		return microsFromAny(strconv.FormatUint(uint64(t), 10))
	case uint16:
		return microsFromAny(strconv.FormatUint(uint64(t), 10))
	case uint32:
		return microsFromAny(strconv.FormatUint(uint64(t), 10))
	case uint64:
		return microsFromAny(strconv.FormatUint(t, 10))
	default:
		if v == nil {
			return 0, false
		}
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" {
			return 0, false
		}
		m, err := arbitrage.ParseMicros(s)
		if err != nil {
			return 0, false
		}
		return m, true
	}
}

func isMatchedOrderResp(resp map[string]any) bool {
	if resp == nil {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(fmt.Sprint(resp["status"])))
	if status == "" {
		return false
	}
	if !strings.Contains(status, "match") && !strings.Contains(status, "fill") {
		return false
	}
	if v, ok := resp["success"]; ok {
		if b, ok := boolFromAny(v); ok && !b {
			return false
		}
	}
	return true
}

func fillFromOrderResp(resp map[string]any) (sharesMicros uint64, valueMicros uint64, ok bool) {
	if !isMatchedOrderResp(resp) {
		return 0, 0, false
	}
	sharesMicros, _ = microsFromAny(resp["makingAmount"])
	valueMicros, _ = microsFromAny(resp["takingAmount"])
	if sharesMicros == 0 {
		return 0, 0, false
	}
	return sharesMicros, valueMicros, true
}

func fillLotsFromTrades(ctx context.Context, c *clob.Client, order *clob.OrderInfo, useServerTime bool) ([]fillLot, uint64, uint64, error) {
	if c == nil || order == nil {
		return nil, 0, 0, fmt.Errorf("nil client or order")
	}
	if len(order.AssociatedTrades) == 0 {
		return nil, 0, 0, nil
	}

	var fills []fillLot
	var totalShares uint64
	var totalCost uint64
	var lastErr error

	orderID := strings.TrimSpace(order.ID)
	orderAsset := strings.TrimSpace(order.AssetID)

	for _, tradeID := range order.AssociatedTrades {
		tradeID = strings.TrimSpace(tradeID)
		if tradeID == "" {
			continue
		}
		trades, err := c.GetTrades(ctx, clob.TradeParams{ID: tradeID}, useServerTime)
		if err != nil {
			lastErr = err
			continue
		}
		for _, tr := range trades {
			if orderID != "" && strings.TrimSpace(tr.TakerOrderID) != "" && !strings.EqualFold(tr.TakerOrderID, orderID) {
				continue
			}
			if orderAsset != "" && strings.TrimSpace(tr.AssetID) != "" && !strings.EqualFold(tr.AssetID, orderAsset) {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(tr.Status), "FAILED") {
				continue
			}

			if len(tr.MakerOrders) > 0 {
				for _, mo := range tr.MakerOrders {
					if orderAsset != "" && strings.TrimSpace(mo.AssetID) != "" && !strings.EqualFold(mo.AssetID, orderAsset) {
						continue
					}
					sharesMicros, err := arbitrage.ParseMicros(mo.MatchedAmount)
					if err != nil || sharesMicros == 0 {
						continue
					}
					priceMicros, err := arbitrage.ParseMicros(mo.Price)
					if err != nil || priceMicros == 0 {
						continue
					}
					fills = append(fills, fillLot{PriceMicros: priceMicros, SharesMicros: sharesMicros})
					totalShares += sharesMicros
					totalCost += arbitrage.CostMicrosForShares(sharesMicros, priceMicros)
				}
				continue
			}

			sharesMicros, err := arbitrage.ParseMicros(tr.Size)
			if err != nil || sharesMicros == 0 {
				continue
			}
			priceMicros, err := arbitrage.ParseMicros(tr.Price)
			if err != nil || priceMicros == 0 {
				continue
			}
			fills = append(fills, fillLot{PriceMicros: priceMicros, SharesMicros: sharesMicros})
			totalShares += sharesMicros
			totalCost += arbitrage.CostMicrosForShares(sharesMicros, priceMicros)
		}
	}

	if totalShares == 0 && lastErr != nil {
		return nil, 0, 0, lastErr
	}
	return fills, totalShares, totalCost, nil
}

func reconcileOrderFills(ctx context.Context, c *clob.Client, orderID string, fallbackPriceMicros uint64, useServerTime bool) ([]fillLot, uint64, uint64, error) {
	if c == nil {
		return nil, 0, 0, fmt.Errorf("nil client")
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, 0, 0, fmt.Errorf("missing order id")
	}

	var lastErr error
	for attempt := 1; attempt <= fillReconAttempts; attempt++ {
		order, err := c.GetOrder(ctx, orderID, useServerTime)
		if err != nil {
			lastErr = err
		} else if order != nil {
			sizeMatched, err := arbitrage.ParseMicros(order.SizeMatched)
			if err != nil {
				sizeMatched = 0
			}
			if sizeMatched == 0 {
				return nil, 0, 0, nil
			}

			fills, filledShares, filledCost, err := fillLotsFromTrades(ctx, c, order, useServerTime)
			if err == nil && filledShares > 0 {
				if filledShares >= sizeMatched || attempt == fillReconAttempts {
					return fills, filledShares, filledCost, nil
				}
			}
			if filledShares == 0 {
				priceMicros := fallbackPriceMicros
				if priceMicros == 0 {
					if p, perr := arbitrage.ParseMicros(order.Price); perr == nil {
						priceMicros = p
					}
				}
				if priceMicros > 0 {
					cost := arbitrage.CostMicrosForShares(sizeMatched, priceMicros)
					return []fillLot{{PriceMicros: priceMicros, SharesMicros: sizeMatched}}, sizeMatched, cost, nil
				}
				return nil, sizeMatched, 0, nil
			}
		}

		if attempt < fillReconAttempts {
			time.Sleep(fillReconDelay)
		}
	}

	if lastErr != nil {
		return nil, 0, 0, lastErr
	}
	return nil, 0, 0, nil
}

func applyBuyFills(s *sideState, fills []fillLot) (filledShares uint64, filledCost uint64) {
	if s == nil {
		return 0, 0
	}
	for _, f := range fills {
		if f.SharesMicros == 0 || f.PriceMicros == 0 {
			continue
		}
		cost := arbitrage.CostMicrosForShares(f.SharesMicros, f.PriceMicros)
		s.spentMicros += cost
		s.heldSharesMicros += f.SharesMicros
		s.totalBoughtMicros += cost
		s.totalBoughtSharesMicros += f.SharesMicros
		s.inv.AddLot(f.PriceMicros, f.SharesMicros)
		filledShares += f.SharesMicros
		filledCost += cost
	}
	return filledShares, filledCost
}

func executeExactBuy(
	ctx context.Context,
	c *clob.Client,
	s *sideState,
	costMicros uint64,
	sharesMicros uint64,
	orderType clob.OrderType,
	useServerTime bool,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	reason string,
) (string, error) {
	if c == nil || s == nil || costMicros == 0 || sharesMicros == 0 {
		err := fmt.Errorf("invalid buy request")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "order",
			Mode:       "live",
			TokenID:    safeTokenID(s),
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	if !c.HasApiCreds() {
		err := fmt.Errorf("api creds not configured")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "order",
			Mode:       "live",
			TokenID:    s.tokenID,
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	rem := remainingCap(s.capMicros, s.spentMicros)
	if rem == 0 {
		return "", nil
	}

	// Use a conservative, cent-quantized collateral amount to avoid cap drift from rounding.
	makerMicros := costMicros
	if makerMicros > rem {
		makerMicros = rem
	}
	if makerMicros > 0 {
		makerMicros = (makerMicros / centMicros) * centMicros
	}
	if makerMicros == 0 {
		return "", nil
	}
	if makerMicros < minBuyMicros {
		if rem < minBuyMicros {
			return "", fmt.Errorf("buy below min order size")
		}
		makerMicros = minBuyMicros
	}

	orderRes, err := c.CreateSignedExactOrder(
		ctx,
		s.tokenID,
		clob.SideBuy,
		new(big.Int).SetUint64(makerMicros),
		new(big.Int).SetUint64(sharesMicros),
		saltGen,
	)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			MakerMicros:  makerMicros,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	if orderRes == nil || orderRes.SignedOrder == nil {
		err := fmt.Errorf("no signed order result")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			MakerMicros:  makerMicros,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}

	body, err := c.BuildPostOrderBody(orderRes.SignedOrder, orderType, false)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			MakerMicros:  makerMicros,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	log.Printf("[post] buy token=%s price=%s maker=%s taker_shares=%s payload=%s", s.tokenID, orderRes.Price, formatMicros(makerMicros), formatShares(sharesMicros), strings.TrimSpace(string(body)))

	resp, _, err := c.PostSignedOrder(ctx, orderRes.SignedOrder, orderType, false, useServerTime)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			MakerMicros:  makerMicros,
			SharesMicros: sharesMicros,
			Price:        orderRes.Price,
			TickSize:     orderRes.TickSize,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	log.Printf("[post] ok token=%s resp=%v", s.tokenID, resp)
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:         time.Now().UnixMilli(),
		Event:        "order",
		Mode:         "live",
		TokenID:      s.tokenID,
		WindowSlug:   windowSlug,
		MakerMicros:  makerMicros,
		SharesMicros: sharesMicros,
		Price:        orderRes.Price,
		TickSize:     orderRes.TickSize,
		OrderType:    string(orderType),
		Resp:         resp,
		Reason:       reason,
		Ok:           true,
		UptimeMs:     time.Since(runStartedAt).Milliseconds(),
	})

	orderID := orderIDFromResp(resp)

	// Fallback amounts if reconciliation fails.
	maker := orderRes.SignedOrder.MakerAmount
	taker := orderRes.SignedOrder.TakerAmount
	if maker == nil || taker == nil || maker.Sign() <= 0 || taker.Sign() <= 0 {
		return orderID, fmt.Errorf("invalid signed order amounts")
	}

	if !maker.IsUint64() {
		return orderID, fmt.Errorf("makerAmount overflows uint64")
	}
	if !taker.IsUint64() {
		return orderID, fmt.Errorf("takerAmount overflows uint64")
	}
	takerU64 := taker.Uint64()

	priceMicros, err := arbitrage.ParseMicros(orderRes.Price)
	if err != nil {
		// Fallback to a derived price if parsing ever fails: maker (collateral) / taker (shares).
		var p big.Int
		var scale big.Int
		scale.SetUint64(microsScale)
		p.Mul(maker, &scale)
		p.Div(&p, taker)
		if !p.IsUint64() {
			return orderID, fmt.Errorf("derived price overflows uint64")
		}
		priceMicros = p.Uint64()
	}

	fills, filledShares, _, fillErr := reconcileOrderFills(ctx, c, orderID, priceMicros, useServerTime)
	if fillErr != nil {
		log.Printf("[warn] buy fill reconcile failed (order=%s): %v; assuming full fill", orderID, fillErr)
		fills = []fillLot{{PriceMicros: priceMicros, SharesMicros: takerU64}}
		filledShares = takerU64
	}
	if filledShares == 0 && len(fills) == 0 {
		return orderID, nil
	}
	if len(fills) == 0 && filledShares > 0 && priceMicros > 0 {
		fills = []fillLot{{PriceMicros: priceMicros, SharesMicros: filledShares}}
	}
	applyBuyFills(s, fills)
	return orderID, nil
}

func executeMarketBuy(
	ctx context.Context,
	c *clob.Client,
	s *sideState,
	costMicros uint64,
	orderType clob.OrderType,
	useServerTime bool,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	reason string,
) (string, error) {
	if c == nil || s == nil || costMicros == 0 {
		err := fmt.Errorf("invalid buy request")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "order",
			Mode:       "live",
			TokenID:    safeTokenID(s),
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	if !c.HasApiCreds() {
		err := fmt.Errorf("api creds not configured")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "order",
			Mode:       "live",
			TokenID:    s.tokenID,
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	rem := remainingCap(s.capMicros, s.spentMicros)
	if rem == 0 {
		return "", nil
	}

	// Use a conservative, cent-quantized collateral amount to avoid cap drift from rounding.
	makerMicros := costMicros
	if makerMicros > rem {
		makerMicros = rem
	}
	if makerMicros > 0 {
		makerMicros = (makerMicros / centMicros) * centMicros
	}
	if makerMicros == 0 {
		return "", nil
	}
	if makerMicros < minBuyMicros {
		if rem < minBuyMicros {
			return "", fmt.Errorf("buy below min order size")
		}
		makerMicros = minBuyMicros
	}

	orderRes, err := c.CreateSignedMarketOrder(
		ctx,
		s.tokenID,
		clob.SideBuy,
		new(big.Int).SetUint64(makerMicros),
		orderType,
		useServerTime,
		saltGen,
	)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:        time.Now().UnixMilli(),
			Event:       "order",
			Mode:        "live",
			TokenID:     s.tokenID,
			WindowSlug:  windowSlug,
			MakerMicros: makerMicros,
			Reason:      reason,
			Ok:          false,
			Err:         err.Error(),
			UptimeMs:    time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	if orderRes == nil || orderRes.SignedOrder == nil {
		err := fmt.Errorf("no signed order result")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:        time.Now().UnixMilli(),
			Event:       "order",
			Mode:        "live",
			TokenID:     s.tokenID,
			WindowSlug:  windowSlug,
			MakerMicros: makerMicros,
			Reason:      reason,
			Ok:          false,
			Err:         err.Error(),
			UptimeMs:    time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}

	taker := orderRes.SignedOrder.TakerAmount
	takerShares := ""
	var takerU64 uint64
	if taker != nil && taker.Sign() > 0 && taker.IsUint64() {
		takerU64 = taker.Uint64()
		takerShares = formatShares(takerU64)
	}

	body, err := c.BuildPostOrderBody(orderRes.SignedOrder, orderType, false)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:        time.Now().UnixMilli(),
			Event:       "order",
			Mode:        "live",
			TokenID:     s.tokenID,
			WindowSlug:  windowSlug,
			MakerMicros: makerMicros,
			Reason:      reason,
			Ok:          false,
			Err:         err.Error(),
			UptimeMs:    time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	log.Printf("[post] buy token=%s price=%s maker=%s taker_shares=%s payload=%s", s.tokenID, orderRes.Price, formatMicros(makerMicros), takerShares, strings.TrimSpace(string(body)))

	resp, _, err := c.PostSignedOrder(ctx, orderRes.SignedOrder, orderType, false, useServerTime)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:        time.Now().UnixMilli(),
			Event:       "order",
			Mode:        "live",
			TokenID:     s.tokenID,
			WindowSlug:  windowSlug,
			MakerMicros: makerMicros,
			Price:       orderRes.Price,
			TickSize:    orderRes.TickSize,
			Reason:      reason,
			Ok:          false,
			Err:         err.Error(),
			UptimeMs:    time.Since(runStartedAt).Milliseconds(),
		})
		return "", err
	}
	log.Printf("[post] ok token=%s resp=%v", s.tokenID, resp)
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:         time.Now().UnixMilli(),
		Event:        "order",
		Mode:         "live",
		TokenID:      s.tokenID,
		WindowSlug:   windowSlug,
		MakerMicros:  makerMicros,
		SharesMicros: takerU64,
		Price:        orderRes.Price,
		TickSize:     orderRes.TickSize,
		OrderType:    string(orderType),
		Resp:         resp,
		Reason:       reason,
		Ok:           true,
		UptimeMs:     time.Since(runStartedAt).Milliseconds(),
	})

	orderID := orderIDFromResp(resp)

	// Fallback amounts if reconciliation fails.
	maker := orderRes.SignedOrder.MakerAmount
	if maker == nil || taker == nil || maker.Sign() <= 0 || taker.Sign() <= 0 {
		return orderID, fmt.Errorf("invalid signed order amounts")
	}

	if !maker.IsUint64() {
		return orderID, fmt.Errorf("makerAmount overflows uint64")
	}
	if !taker.IsUint64() {
		return orderID, fmt.Errorf("takerAmount overflows uint64")
	}
	if takerU64 == 0 {
		takerU64 = taker.Uint64()
	}

	priceMicros, err := arbitrage.ParseMicros(orderRes.Price)
	if err != nil {
		// Fallback to a derived price if parsing ever fails: maker (collateral) / taker (shares).
		var p big.Int
		var scale big.Int
		scale.SetUint64(microsScale)
		p.Mul(maker, &scale)
		p.Div(&p, taker)
		if !p.IsUint64() {
			return orderID, fmt.Errorf("derived price overflows uint64")
		}
		priceMicros = p.Uint64()
	}

	fills, filledShares, _, fillErr := reconcileOrderFills(ctx, c, orderID, priceMicros, useServerTime)
	if fillErr != nil {
		log.Printf("[warn] buy fill reconcile failed (order=%s): %v; assuming full fill", orderID, fillErr)
		fills = []fillLot{{PriceMicros: priceMicros, SharesMicros: takerU64}}
		filledShares = takerU64
	}
	if filledShares == 0 && len(fills) == 0 {
		return orderID, nil
	}
	if len(fills) == 0 && filledShares > 0 && priceMicros > 0 {
		fills = []fillLot{{PriceMicros: priceMicros, SharesMicros: filledShares}}
	}
	applyBuyFills(s, fills)
	return orderID, nil
}

func executeMarketSell(
	ctx context.Context,
	c *clob.Client,
	s *sideState,
	sharesMicros uint64,
	slippageBps int,
	orderType clob.OrderType,
	useServerTime bool,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	windowSlug string,
	reason string,
) (filledShares uint64, filledValueMicros uint64, err error) {
	if c == nil || s == nil || sharesMicros == 0 {
		err := fmt.Errorf("invalid sell request")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "sell_order",
			Mode:       "live",
			TokenID:    safeTokenID(s),
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}
	if sharesMicros < minSellSharesMicros {
		err := fmt.Errorf("sell below min order size")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "sell_order",
			Mode:         "live",
			TokenID:      safeTokenID(s),
			WindowSlug:   windowSlug,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}
	if !c.HasApiCreds() {
		err := fmt.Errorf("api creds not configured")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:       time.Now().UnixMilli(),
			Event:      "sell_order",
			Mode:       "live",
			TokenID:    s.tokenID,
			WindowSlug: windowSlug,
			Reason:     reason,
			Ok:         false,
			Err:        err.Error(),
			UptimeMs:   time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}

	var orderRes *clob.OrderResult
	if slippageBps > 0 {
		orderRes, err = c.CreateSignedMarketOrderWithSlippage(
			ctx,
			s.tokenID,
			clob.SideSell,
			new(big.Int).SetUint64(sharesMicros),
			orderType,
			useServerTime,
			slippageBps,
			saltGen,
		)
	} else {
		orderRes, err = c.CreateSignedMarketOrder(
			ctx,
			s.tokenID,
			clob.SideSell,
			new(big.Int).SetUint64(sharesMicros),
			orderType,
			useServerTime,
			saltGen,
		)
	}
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "sell_order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}
	if orderRes == nil || orderRes.SignedOrder == nil {
		err := fmt.Errorf("no signed order result")
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "sell_order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}

	body, err := c.BuildPostOrderBody(orderRes.SignedOrder, orderType, false)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "sell_order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			SharesMicros: sharesMicros,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}
	log.Printf("[post] sell token=%s price=%s maker_shares=%s payload=%s", s.tokenID, orderRes.Price, formatShares(sharesMicros), strings.TrimSpace(string(body)))

	resp, _, err := c.PostSignedOrder(ctx, orderRes.SignedOrder, orderType, false, useServerTime)
	if err != nil {
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:         time.Now().UnixMilli(),
			Event:        "sell_order",
			Mode:         "live",
			TokenID:      s.tokenID,
			WindowSlug:   windowSlug,
			SharesMicros: sharesMicros,
			Price:        orderRes.Price,
			TickSize:     orderRes.TickSize,
			Reason:       reason,
			Ok:           false,
			Err:          err.Error(),
			UptimeMs:     time.Since(runStartedAt).Milliseconds(),
		})
		return 0, 0, err
	}
	log.Printf("[post] ok sell token=%s resp=%v", s.tokenID, resp)
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:         time.Now().UnixMilli(),
		Event:        "sell_order",
		Mode:         "live",
		TokenID:      s.tokenID,
		WindowSlug:   windowSlug,
		SharesMicros: sharesMicros,
		Price:        orderRes.Price,
		TickSize:     orderRes.TickSize,
		OrderType:    string(orderType),
		Resp:         resp,
		Reason:       reason,
		Ok:           true,
		UptimeMs:     time.Since(runStartedAt).Milliseconds(),
	})

	if filledShares, filledValue, ok := fillFromOrderResp(resp); ok {
		if filledValue == 0 {
			if priceMicros, perr := arbitrage.ParseMicros(orderRes.Price); perr == nil && priceMicros > 0 {
				filledValue = arbitrage.CostMicrosForShares(filledShares, priceMicros)
			}
		}
		return filledShares, filledValue, nil
	}

	orderID := orderIDFromResp(resp)

	// Fallback price for fill reconciliation.
	priceMicros, parseErr := arbitrage.ParseMicros(orderRes.Price)
	if parseErr != nil {
		priceMicros = 0
		maker := orderRes.SignedOrder.MakerAmount
		taker := orderRes.SignedOrder.TakerAmount
		if maker != nil && taker != nil && maker.Sign() > 0 && taker.Sign() > 0 {
			var p big.Int
			var scale big.Int
			scale.SetUint64(microsScale)
			p.Mul(taker, &scale)
			p.Div(&p, maker)
			if p.IsUint64() {
				priceMicros = p.Uint64()
			}
		}
	}

	fills, filledShares, filledValueMicros, fillErr := reconcileOrderFills(ctx, c, orderID, priceMicros, useServerTime)
	if fillErr != nil {
		log.Printf("[warn] sell fill reconcile failed (order=%s): %v", orderID, fillErr)
		return 0, 0, nil
	}
	if filledShares == 0 && len(fills) == 0 {
		return 0, 0, nil
	}
	if filledValueMicros == 0 && filledShares > 0 && priceMicros > 0 {
		filledValueMicros = arbitrage.CostMicrosForShares(filledShares, priceMicros)
	}
	return filledShares, filledValueMicros, nil
}

func printStatus(bestA, bestB arbitrage.BestAsk, st botState, bookA, bookB *clob.OrderBookSummary) {
	bestBidA, okBidA := bestBidPriceMicrosFromBook(bookA)
	bestBidB, okBidB := bestBidPriceMicrosFromBook(bookB)
	heldA := st.a.heldSharesMicros
	heldB := st.b.heldSharesMicros
	missingBid := (heldA > 0 && !okBidA) || (heldB > 0 && !okBidB)
	positionValue := uint64(0)
	if heldA > 0 && okBidA {
		positionValue += mulDivU64(heldA, bestBidA, microsScale)
	}
	if heldB > 0 && okBidB {
		positionValue += mulDivU64(heldB, bestBidB, microsScale)
	}
	invested := st.a.spentMicros + st.b.spentMicros
	pnlPct := "na"
	markStr := "na"
	if invested > 0 && !missingBid {
		pnlBps := pnlBpsFromMicros(invested, positionValue)
		pnlPct = fmt.Sprintf("%.4f%%", float64(pnlBps)/100.0)
		markStr = formatMicros(positionValue)
	}

	log.Printf(
		"[status] A ask=%s/%s spent=%s/%s held=%s bought=%s/%s | B ask=%s/%s spent=%s/%s held=%s bought=%s/%s | mark=%s pnl_pct=%s",
		formatMicros(bestA.PriceMicros),
		formatShares(bestA.SharesMicros),
		formatMicros(st.a.spentMicros),
		formatMicros(st.a.capMicros),
		formatShares(heldA),
		formatMicros(st.a.totalBoughtMicros),
		formatShares(st.a.totalBoughtSharesMicros),
		formatMicros(bestB.PriceMicros),
		formatShares(bestB.SharesMicros),
		formatMicros(st.b.spentMicros),
		formatMicros(st.b.capMicros),
		formatShares(heldB),
		formatMicros(st.b.totalBoughtMicros),
		formatShares(st.b.totalBoughtSharesMicros),
		markStr,
		pnlPct,
	)
}

func formatMicros(m uint64) string {
	whole := m / microsScale
	frac := m % microsScale
	if frac == 0 {
		return fmt.Sprintf("%d", whole)
	}
	fs := fmt.Sprintf("%06d", frac)
	fs = strings.TrimRight(fs, "0")
	return fmt.Sprintf("%d.%s", whole, fs)
}

func formatShares(sharesMicros uint64) string {
	return formatMicros(sharesMicros)
}

func formatAllowance(m uint64) string {
	if m == math.MaxUint64 {
		return "max"
	}
	return formatMicros(m)
}

func logBalanceAllowanceSnapshot(ctx context.Context, c *clob.Client, st *botState, neededMicros uint64) {
	if c == nil || st == nil {
		return
	}
	now := time.Now()
	if !st.lastBalanceAllowanceLogAt.IsZero() && now.Sub(st.lastBalanceAllowanceLogAt) < 10*time.Second {
		return
	}
	st.lastBalanceAllowanceLogAt = now

	rpcURL, err := polygonutil.RPCURLFromEnv()
	if err != nil {
		log.Printf("[warn] balance/allowance snapshot skipped: %v", err)
		return
	}
	contracts, err := orderconfig.GetContracts(c.ChainID())
	if err != nil {
		log.Printf("[warn] balance/allowance snapshot skipped: %v", err)
		return
	}
	funder := c.FunderAddress()
	balCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	balance, allowances, err := polygonutil.USDCTokenBalanceAndAllowancesMicros(
		balCtx,
		rpcURL,
		funder,
		[]common.Address{contracts.Exchange, contracts.NegRiskExchange},
	)
	if err != nil {
		log.Printf("[warn] balance/allowance snapshot failed: %v", err)
		return
	}
	log.Printf(
		"[warn] balance/allowance snapshot funder=%s needed=%s usdc_balance=%s allowance_exchange=%s allowance_neg_risk=%s",
		funder.Hex(),
		formatMicros(neededMicros),
		formatMicros(balance),
		formatAllowance(allowances[contracts.Exchange]),
		formatAllowance(allowances[contracts.NegRiskExchange]),
	)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func safePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func isNotEnoughBalanceOrAllowance(msg string) bool {
	msg = strings.ToLower(strings.TrimSpace(msg))
	if msg == "" {
		return false
	}
	if strings.Contains(msg, "not enough balance") && strings.Contains(msg, "allowance") {
		return true
	}
	if strings.Contains(msg, "balance / allowance") || strings.Contains(msg, "balance/allowance") {
		return true
	}
	// Fallback: accept any message that explicitly mentions both balance and allowance.
	return strings.Contains(msg, "balance") && strings.Contains(msg, "allowance")
}

func parsePrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	hexKey = strings.TrimSpace(hexKey)
	if hexKey == "" {
		return nil, fmt.Errorf("private key missing")
	}
	hexKey = strings.TrimPrefix(hexKey, "0x")
	pk, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}
	return pk, nil
}

func parseOrGeneratePrivateKey(hexKey string) (*ecdsa.PrivateKey, bool, error) {
	hexKey = strings.TrimSpace(hexKey)
	if hexKey != "" {
		pk, err := parsePrivateKey(hexKey)
		return pk, false, err
	}
	pk, err := crypto.GenerateKey()
	if err != nil {
		return nil, false, fmt.Errorf("generate ephemeral key: %w", err)
	}
	return pk, true, nil
}
