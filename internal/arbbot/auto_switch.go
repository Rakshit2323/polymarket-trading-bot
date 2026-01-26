package arbbot

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "time/tzdata"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/gamma"
	"poly-gocopy/internal/jsonl"
)

type windowedSlug struct {
	stem     string
	interval time.Duration
	kind     windowedSlugKind
	offset   int
}

type windowedSlugKind uint8

const (
	windowedSlugKindUnixSuffix windowedSlugKind = iota
	windowedSlugKindHourlyEtHumanSuffix
)

func parseWindowedSlug(eventSlug string) (windowedSlug, error) {
	eventSlug = strings.TrimSpace(eventSlug)
	if eventSlug == "" {
		return windowedSlug{}, fmt.Errorf("event slug required")
	}

	eventSlug, offset, err := parseWindowOffsetSuffix(eventSlug)
	if err != nil {
		return windowedSlug{}, err
	}

	// Hourly markets on polymarket.com/crypto/hourly use a human-readable ET suffix:
	//   bitcoin-up-or-down-december-15-3pm-et
	// Accept either the full slug (above) or the stem (bitcoin-up-or-down).
	if strings.Contains(eventSlug, "-up-or-down") {
		stem := eventSlug
		if stripped, ok := stripHourlyEtSuffix(eventSlug); ok {
			stem = stripped
		}
		if stem == "" || !strings.Contains(stem, "-up-or-down") {
			return windowedSlug{}, fmt.Errorf("invalid hourly slug stem")
		}
		return windowedSlug{
			stem:     stem,
			interval: time.Hour,
			kind:     windowedSlugKindHourlyEtHumanSuffix,
			offset:   offset,
		}, nil
	}

	// Accept either:
	// - "btc-updown-15m" (preferred), or
	// - "btc-updown-15m-1765791900" (legacy; timestamp portion is ignored and recomputed).
	stem := eventSlug
	lastDash := strings.LastIndex(eventSlug, "-")
	if lastDash > 0 && lastDash < len(eventSlug)-1 {
		lastPart := strings.TrimSpace(eventSlug[lastDash+1:])
		if _, err := strconv.ParseInt(lastPart, 10, 64); err == nil {
			stem = strings.TrimSpace(eventSlug[:lastDash])
		}
	}

	if stem == "" {
		return windowedSlug{}, fmt.Errorf("invalid slug stem")
	}

	// Assume the last hyphen-separated segment of the stem is a duration string (e.g. "15m").
	dash2 := strings.LastIndex(stem, "-")
	if dash2 <= 0 || dash2 == len(stem)-1 {
		return windowedSlug{}, fmt.Errorf("expected stem to end with -<duration> (e.g. -15m)")
	}
	durStr := strings.TrimSpace(stem[dash2+1:])
	durStr = normalizeDurationStringForParse(durStr)
	interval, err := time.ParseDuration(durStr)
	if err != nil {
		return windowedSlug{}, fmt.Errorf("parse duration %q: %w", durStr, err)
	}
	if interval <= 0 {
		return windowedSlug{}, fmt.Errorf("duration must be > 0")
	}
	if interval%time.Minute != 0 {
		return windowedSlug{}, fmt.Errorf("duration must be a whole number of minutes, got %s", interval)
	}

	return windowedSlug{
		stem:     stem,
		interval: interval,
		kind:     windowedSlugKindUnixSuffix,
		offset:   offset,
	}, nil
}

func parseWindowOffsetSuffix(slug string) (base string, offset int, err error) {
	// Optional syntax: "<slug>+<n>", where n is a non-negative integer number of
	// window intervals to look ahead.
	//
	// Example:
	//   btc-updown-15m+1
	//   bitcoin-up-or-down+1
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return "", 0, fmt.Errorf("event slug required")
	}
	plus := strings.LastIndex(slug, "+")
	if plus < 0 {
		return slug, 0, nil
	}
	if plus == len(slug)-1 {
		return "", 0, fmt.Errorf("invalid event slug %q: trailing '+'", slug)
	}
	base = strings.TrimSpace(slug[:plus])
	if base == "" {
		return "", 0, fmt.Errorf("invalid event slug %q: empty base before '+'", slug)
	}
	raw := strings.TrimSpace(slug[plus+1:])
	n, convErr := strconv.Atoi(raw)
	if convErr != nil || n < 0 {
		return "", 0, fmt.Errorf("invalid event slug %q: bad offset %q (expected +<n>)", slug, raw)
	}
	return base, n, nil
}

func windowStartForNow(now time.Time, ws windowedSlug, loc *time.Location) time.Time {
	start := floorToIntervalInLocation(now, ws.interval, loc)
	if ws.offset == 0 {
		return start
	}
	return start.Add(time.Duration(ws.offset) * ws.interval)
}

func floorToIntervalInLocation(t time.Time, interval time.Duration, loc *time.Location) time.Time {
	if interval <= 0 {
		return t
	}
	if loc == nil {
		return t
	}

	lt := t.In(loc)
	y, m, d := lt.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, loc)
	delta := lt.Sub(midnight)
	if delta <= 0 {
		return midnight
	}

	floored := delta - (delta % interval)
	return midnight.Add(floored)
}

func (w windowedSlug) slugForWindowStart(start time.Time) string {
	switch w.kind {
	case windowedSlugKindHourlyEtHumanSuffix:
		lt := start.In(start.Location())
		month := strings.ToLower(lt.Month().String())
		day := strconv.Itoa(lt.Day())
		h := lt.Hour()
		ampm := "am"
		if h >= 12 {
			ampm = "pm"
		}
		h12 := h % 12
		if h12 == 0 {
			h12 = 12
		}
		clock := strconv.Itoa(h12) + ampm
		return w.stem + "-" + month + "-" + day + "-" + clock + "-et"
	default:
		return w.stem + "-" + strconv.FormatInt(start.Unix(), 10)
	}
}

func runAutoSwitchLoop(ctx context.Context, clobClient *clob.Client, parsed args, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, buyStep buyStepFunc) error {
	if parsed.nextSlugLast > 0 {
		return runAutoSwitchLoopLookahead(ctx, clobClient, parsed, saltGen, tradeLog, runStartedAt, buyStep)
	}

	locName := strings.TrimSpace(parsed.eventTZ)
	if locName == "" {
		locName = "America/New_York"
	}
	loc, err := time.LoadLocation(locName)
	if err != nil {
		return fmt.Errorf("load timezone %q: %w", locName, err)
	}

	ws, err := parseWindowedSlug(parsed.eventSlug)
	if err != nil {
		return fmt.Errorf("invalid --event-slug %q: %w", parsed.eventSlug, err)
	}

	gammaClient, err := gamma.NewClient(parsed.gammaURL)
	if err != nil {
		return fmt.Errorf("gamma client: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		// Resolve the current rolling-window slug and token IDs.
		start := windowStartForNow(time.Now(), ws, loc)
		slug := ws.slugForWindowStart(start)

		resolved, err := resolveMarketWithRetry(ctx, gammaClient, slug)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		// Avoid starting a session for a window that already advanced while resolving.
		startNow := windowStartForNow(time.Now(), ws, loc)
		slugNow := ws.slugForWindowStart(startNow)
		if slugNow != slug {
			log.Printf("[ctx] window advanced during resolve: %s -> %s (retry)", slug, slugNow)
			continue
		}

		tokenA := resolved.TokenIDs[0]
		tokenB := resolved.TokenIDs[1]

		log.Printf("[ctx] event=%s start_et=%s interval=%s outcomes=%v", slug, startNow.In(loc).Format("2006-01-02 15:04:05"), ws.interval, resolved.Outcomes)
		log.Printf("[ctx] tokens: A=%s B=%s", tokenA, tokenB)
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:          time.Now().UnixMilli(),
			Event:         "session_start",
			Mode:          arbMode(parsed.enableTrading),
			Source:        parsed.source,
			EventSlug:     parsed.eventSlug,
			WindowSlug:    slug,
			TokenA:        tokenA,
			TokenB:        tokenB,
			CapAMicros:    parsed.capAMicros,
			CapBMicros:    parsed.capBMicros,
			OrderType:     string(parsed.orderType),
			EnableTrading: parsed.enableTrading,
			UptimeMs:      time.Since(runStartedAt).Milliseconds(),
		})

		sessionArgs := parsed
		sessionArgs.tokenA = tokenA
		sessionArgs.tokenB = tokenB

		st := botState{
			a: sideState{
				tokenID:   tokenA,
				capMicros: sessionArgs.capAMicros,
			},
			b: sideState{
				tokenID:   tokenB,
				capMicros: sessionArgs.capBMicros,
			},
		}

		nextSwitch := startNow.Add(ws.interval)
		wait := time.Until(nextSwitch)
		if wait < 0 {
			wait = 0
		}

		sessionCtx, sessionCancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			switch sessionArgs.source {
			case "poll":
				runPollLoop(sessionCtx, clobClient, sessionArgs, &st, saltGen, tradeLog, runStartedAt, slug, startNow, nextSwitch, buyStep)
			case "rtds":
				runRTDSLoop(sessionCtx, clobClient, sessionArgs, &st, saltGen, tradeLog, runStartedAt, slug, startNow, nextSwitch, buyStep)
			default:
				log.Printf("[warn] unknown source %q", sessionArgs.source)
			}
		}()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			sessionCancel()
			<-done
			logArbEvent(tradeLog, arbLogEvent{
				TsMs:         time.Now().UnixMilli(),
				Event:        "session_end",
				Mode:         arbMode(parsed.enableTrading),
				Source:       parsed.source,
				EventSlug:    parsed.eventSlug,
				WindowSlug:   slug,
				TokenA:       tokenA,
				TokenB:       tokenB,
				CapAMicros:   st.a.capMicros,
				CapBMicros:   st.b.capMicros,
				SpentAMicros: st.a.spentMicros,
				SpentBMicros: st.b.spentMicros,
				UptimeMs:     time.Since(runStartedAt).Milliseconds(),
			})
			return nil
		case <-timer.C:
			sessionCancel()
			<-done
			logArbEvent(tradeLog, arbLogEvent{
				TsMs:         time.Now().UnixMilli(),
				Event:        "session_end",
				Mode:         arbMode(parsed.enableTrading),
				Source:       parsed.source,
				EventSlug:    parsed.eventSlug,
				WindowSlug:   slug,
				TokenA:       tokenA,
				TokenB:       tokenB,
				CapAMicros:   st.a.capMicros,
				CapBMicros:   st.b.capMicros,
				SpentAMicros: st.a.spentMicros,
				SpentBMicros: st.b.spentMicros,
				UptimeMs:     time.Since(runStartedAt).Milliseconds(),
			})
			// Loop will compute and start the next window.
		}
	}
}

type autoSwitchSession struct {
	args       args
	windowSlug string
	windowFrom time.Time
	windowTo   time.Time

	tokenA string
	tokenB string

	st *botState

	cancel context.CancelFunc
	done   chan struct{}
}

func startAutoSwitchSession(
	ctx context.Context,
	clobClient *clob.Client,
	gammaClient *gamma.Client,
	ws windowedSlug,
	loc *time.Location,
	parsed args,
	windowStart time.Time,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	buyStep buyStepFunc,
) (*autoSwitchSession, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if clobClient == nil {
		return nil, fmt.Errorf("clob client nil")
	}
	if gammaClient == nil {
		return nil, fmt.Errorf("gamma client nil")
	}

	windowSlug := ws.slugForWindowStart(windowStart)
	resolved, err := resolveMarketWithRetry(ctx, gammaClient, windowSlug)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	// Ensure we didn't resolve a stale window (window advanced during resolve).
	baseNow := floorToIntervalInLocation(time.Now(), ws.interval, loc)
	if ws.offset != 0 {
		baseNow = baseNow.Add(time.Duration(ws.offset) * ws.interval)
	}
	if !windowStart.Equal(baseNow) && !windowStart.Equal(baseNow.Add(ws.interval)) {
		return nil, fmt.Errorf("window advanced during resolve (start=%s now_base=%s)", windowStart.In(loc).Format(time.RFC3339), baseNow.In(loc).Format(time.RFC3339))
	}

	if len(resolved.TokenIDs) < 2 {
		return nil, fmt.Errorf("gamma returned <2 token IDs for %q", windowSlug)
	}
	tokenA := resolved.TokenIDs[0]
	tokenB := resolved.TokenIDs[1]

	windowEnd := windowStart.Add(ws.interval)

	log.Printf("[ctx] event=%s start_et=%s interval=%s outcomes=%v", windowSlug, windowStart.In(loc).Format("2006-01-02 15:04:05"), ws.interval, resolved.Outcomes)
	log.Printf("[ctx] tokens: A=%s B=%s", tokenA, tokenB)
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:          time.Now().UnixMilli(),
		Event:         "session_start",
		Mode:          arbMode(parsed.enableTrading),
		Source:        parsed.source,
		EventSlug:     parsed.eventSlug,
		WindowSlug:    windowSlug,
		TokenA:        tokenA,
		TokenB:        tokenB,
		CapAMicros:    parsed.capAMicros,
		CapBMicros:    parsed.capBMicros,
		OrderType:     string(parsed.orderType),
		EnableTrading: parsed.enableTrading,
		UptimeMs:      time.Since(runStartedAt).Milliseconds(),
	})

	sessionArgs := parsed
	sessionArgs.tokenA = tokenA
	sessionArgs.tokenB = tokenB

	st := botState{
		a: sideState{
			tokenID:   tokenA,
			capMicros: sessionArgs.capAMicros,
		},
		b: sideState{
			tokenID:   tokenB,
			capMicros: sessionArgs.capBMicros,
		},
	}

	sessionCtx, sessionCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func(a args, st *botState) {
		defer close(done)
		switch a.source {
		case "poll":
			runPollLoop(sessionCtx, clobClient, a, st, saltGen, tradeLog, runStartedAt, windowSlug, windowStart, windowEnd, buyStep)
		case "rtds":
			runRTDSLoop(sessionCtx, clobClient, a, st, saltGen, tradeLog, runStartedAt, windowSlug, windowStart, windowEnd, buyStep)
		default:
			log.Printf("[warn] unknown source %q", a.source)
		}
	}(sessionArgs, &st)

	return &autoSwitchSession{
		args:       sessionArgs,
		windowSlug: windowSlug,
		windowFrom: windowStart,
		windowTo:   windowEnd,
		tokenA:     tokenA,
		tokenB:     tokenB,
		st:         &st,
		cancel:     sessionCancel,
		done:       done,
	}, nil
}

func stopAutoSwitchSession(s *autoSwitchSession, tradeLog *jsonl.Writer, runStartedAt time.Time) {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.done != nil {
		<-s.done
	}
	logArbEvent(tradeLog, arbLogEvent{
		TsMs:         time.Now().UnixMilli(),
		Event:        "session_end",
		Mode:         arbMode(s.args.enableTrading),
		Source:       s.args.source,
		EventSlug:    s.args.eventSlug,
		WindowSlug:   s.windowSlug,
		TokenA:       s.tokenA,
		TokenB:       s.tokenB,
		CapAMicros:   s.st.a.capMicros,
		CapBMicros:   s.st.b.capMicros,
		SpentAMicros: s.st.a.spentMicros,
		SpentBMicros: s.st.b.spentMicros,
		UptimeMs:     time.Since(runStartedAt).Milliseconds(),
	})
}

func runAutoSwitchLoopLookahead(ctx context.Context, clobClient *clob.Client, parsed args, saltGen func() int64, tradeLog *jsonl.Writer, runStartedAt time.Time, buyStep buyStepFunc) error {
	locName := strings.TrimSpace(parsed.eventTZ)
	if locName == "" {
		locName = "America/New_York"
	}
	loc, err := time.LoadLocation(locName)
	if err != nil {
		return fmt.Errorf("load timezone %q: %w", locName, err)
	}

	ws, err := parseWindowedSlug(parsed.eventSlug)
	if err != nil {
		return fmt.Errorf("invalid --event-slug %q: %w", parsed.eventSlug, err)
	}

	gammaClient, err := gamma.NewClient(parsed.gammaURL)
	if err != nil {
		return fmt.Errorf("gamma client: %w", err)
	}

	var (
		cur            *autoSwitchSession
		next           *autoSwitchSession
		nextStartTried bool
	)

	for {
		if ctx.Err() != nil {
			stopAutoSwitchSession(cur, tradeLog, runStartedAt)
			stopAutoSwitchSession(next, tradeLog, runStartedAt)
			return nil
		}

		if cur == nil {
			// Start the current window.
			start := windowStartForNow(time.Now(), ws, loc)
			s, err := startAutoSwitchSession(ctx, clobClient, gammaClient, ws, loc, parsed, start, saltGen, tradeLog, runStartedAt, buyStep)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("[warn] start session: %v", err)
				time.Sleep(250 * time.Millisecond)
				continue
			}
			cur = s
			next = nil
			nextStartTried = false
		}

		now := time.Now()

		actionAt := cur.windowTo
		action := "end"
		if parsed.nextSlugLast > 0 && next == nil && !nextStartTried {
			prefetchAt := cur.windowTo.Add(-parsed.nextSlugLast)
			if prefetchAt.Before(actionAt) {
				actionAt = prefetchAt
				action = "prefetch"
			}
		}
		if actionAt.Before(now) {
			actionAt = now
		}

		timer := time.NewTimer(actionAt.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			stopAutoSwitchSession(cur, tradeLog, runStartedAt)
			stopAutoSwitchSession(next, tradeLog, runStartedAt)
			return nil

		case <-timer.C:
			if action == "prefetch" {
				// Start trading the next window, but only once we are inside the last
				// parsed.nextSlugLast portion of the current window.
				nextStartTried = true
				nextStart := cur.windowFrom.Add(ws.interval)
				s, err := startAutoSwitchSession(ctx, clobClient, gammaClient, ws, loc, parsed, nextStart, saltGen, tradeLog, runStartedAt, buyStep)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					log.Printf("[warn] next window start failed: %v", err)
					// Give up for this window; we'll try again after the window rolls.
				} else {
					next = s
					log.Printf("[ctx] lookahead started: %s (current ends in %s)", next.windowSlug, time.Until(cur.windowTo).Truncate(time.Second))
				}
				continue
			}

			// Window end.
			oldCur := cur
			stopAutoSwitchSession(oldCur, tradeLog, runStartedAt)
			cur = nil

			if next != nil && next.windowFrom.Equal(oldCur.windowFrom.Add(ws.interval)) {
				cur = next
				next = nil
				nextStartTried = false
				log.Printf("[ctx] lookahead promoted: %s", cur.windowSlug)
			}
		}
	}
}

type slugRunner struct {
	id     uint64
	cancel context.CancelFunc
}

type runnerExit struct {
	slug string
	id   uint64
	err  error
}

func runAutoSwitchManager(
	ctx context.Context,
	cancel context.CancelFunc,
	reload <-chan struct{},
	clobClient *clob.Client,
	parsed args,
	saltGen func() int64,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
	buyStep buyStepFunc,
) error {
	if strings.TrimSpace(parsed.eventSlugsFile) == "" && len(parsed.eventSlugs) == 0 {
		return fmt.Errorf("no event slugs configured")
	}

	if err := validateWindowedSlugs(parsed.eventSlugs); err != nil {
		return err
	}

	var nextRunnerID atomic.Uint64
	runners := make(map[string]slugRunner, len(parsed.eventSlugs))

	exits := make(chan runnerExit, 64)
	startRunner := func(slug string) {
		id := nextRunnerID.Add(1)
		runnerCtx, runnerCancel := context.WithCancel(ctx)
		runners[slug] = slugRunner{id: id, cancel: runnerCancel}

		sessionArgs := parsed
		sessionArgs.eventSlug = slug
		sessionArgs.eventSlugs = nil

		go func(slug string, id uint64, a args) {
			err := runAutoSwitchLoop(runnerCtx, clobClient, a, saltGen, tradeLog, runStartedAt, buyStep)
			select {
			case exits <- runnerExit{slug: slug, id: id, err: err}:
			case <-ctx.Done():
			}
		}(slug, id, sessionArgs)
	}

	applyDesired := func(desired []string) {
		desiredSet := make(map[string]struct{}, len(desired))
		for _, slug := range desired {
			desiredSet[slug] = struct{}{}
		}

		// Stop removed slugs.
		for slug, r := range runners {
			if _, ok := desiredSet[slug]; ok {
				continue
			}
			r.cancel()
			delete(runners, slug)
			log.Printf("[cfg] stopped slug %q", slug)
		}

		// Start new slugs.
		for _, slug := range desired {
			if _, ok := runners[slug]; ok {
				continue
			}
			startRunner(slug)
			log.Printf("[cfg] started slug %q", slug)
		}
	}

	applyDesired(parsed.eventSlugs)

	for {
		select {
		case <-ctx.Done():
			for _, r := range runners {
				r.cancel()
			}
			return nil

		case <-reload:
			path := strings.TrimSpace(parsed.eventSlugsFile)
			if path == "" {
				log.Printf("[cfg] reload requested, but no slugs file configured")
				continue
			}
			newSlugs, err := readSlugsFile(path)
			if err != nil {
				log.Printf("[warn] reload slugs file %q failed: %v", path, err)
				continue
			}
			if err := validateWindowedSlugs(newSlugs); err != nil {
				log.Printf("[warn] reload slugs file %q ignored: %v", path, err)
				continue
			}

			log.Printf("[cfg] reloaded slugs file %q: %d slugs", path, len(newSlugs))
			logArbEvent(tradeLog, arbLogEvent{
				TsMs:       time.Now().UnixMilli(),
				Event:      "reload_slugs",
				Mode:       arbMode(parsed.enableTrading),
				Source:     parsed.source,
				EventSlugs: append([]string(nil), newSlugs...),
				UptimeMs:   time.Since(runStartedAt).Milliseconds(),
			})

			parsed.eventSlugs = newSlugs
			applyDesired(newSlugs)

		case ex := <-exits:
			r, ok := runners[ex.slug]
			if !ok || r.id != ex.id {
				continue
			}
			delete(runners, ex.slug)

			if ex.err != nil && ctx.Err() == nil {
				if cancel != nil {
					cancel()
				}
				return fmt.Errorf("auto-switch %q stopped: %w", ex.slug, ex.err)
			}
			if ex.err == nil && ctx.Err() == nil {
				if cancel != nil {
					cancel()
				}
				return fmt.Errorf("auto-switch %q stopped unexpectedly", ex.slug)
			}
		}
	}
}

func validateWindowedSlugs(slugs []string) error {
	for _, slug := range slugs {
		if _, err := parseWindowedSlug(slug); err != nil {
			return fmt.Errorf("invalid event slug %q: %w", slug, err)
		}
	}
	return nil
}

func normalizeDurationStringForParse(durStr string) string {
	durStr = strings.TrimSpace(durStr)
	if durStr == "" {
		return ""
	}
	durStrLower := strings.ToLower(durStr)
	if strings.HasSuffix(durStrLower, "hr") {
		return strings.TrimSuffix(durStrLower, "hr") + "h"
	}
	if strings.HasSuffix(durStrLower, "hrs") {
		return strings.TrimSuffix(durStrLower, "hrs") + "h"
	}
	return durStrLower
}

func stripHourlyEtSuffix(slug string) (stem string, ok bool) {
	// Example:
	//   bitcoin-up-or-down-december-15-3pm-et
	// -> bitcoin-up-or-down
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return "", false
	}

	parts := strings.Split(slug, "-")
	if len(parts) < 5 {
		return "", false
	}

	// trailing ...-<month>-<day>-<h(am|pm)>-et
	if strings.ToLower(parts[len(parts)-1]) != "et" {
		return "", false
	}
	timePart := strings.ToLower(parts[len(parts)-2])
	dayPart := parts[len(parts)-3]
	monthPart := strings.ToLower(parts[len(parts)-4])

	if !isMonthName(monthPart) {
		return "", false
	}
	day, err := strconv.Atoi(dayPart)
	if err != nil || day <= 0 || day > 31 {
		return "", false
	}
	if !isHourAmPm(timePart) {
		return "", false
	}

	stemParts := parts[:len(parts)-4]
	stem = strings.Join(stemParts, "-")
	stem = strings.TrimSpace(stem)
	if stem == "" {
		return "", false
	}
	return stem, true
}

func isMonthName(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december":
		return true
	default:
		return false
	}
}

func isHourAmPm(s string) bool {
	// 1am..12am, 1pm..12pm
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) < 3 {
		return false
	}
	suffix := s[len(s)-2:]
	if suffix != "am" && suffix != "pm" {
		return false
	}
	hourStr := s[:len(s)-2]
	h, err := strconv.Atoi(hourStr)
	if err != nil || h < 1 || h > 12 {
		return false
	}
	return true
}

func resolveMarketWithRetry(ctx context.Context, c *gamma.Client, slug string) (gamma.ResolvedMarket, error) {
	backoff := 300 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		if ctx.Err() != nil {
			return gamma.ResolvedMarket{}, ctx.Err()
		}

		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		res, err := c.ResolveMarketBySlug(reqCtx, slug)
		cancel()
		if err == nil {
			return res, nil
		}

		log.Printf("[warn] gamma resolve %q failed: %v", slug, err)
		if err := sleepWithContext(ctx, backoff); err != nil {
			return gamma.ResolvedMarket{}, err
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
