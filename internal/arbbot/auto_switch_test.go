package arbbot

import (
	"testing"
	"time"
)

func TestParseWindowedSlug(t *testing.T) {
	ws, err := parseWindowedSlug("btc-updown-15m")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.stem != "btc-updown-15m" {
		t.Fatalf("unexpected stem: %q", ws.stem)
	}
	if ws.interval != 15*time.Minute {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 0 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}
}

func TestParseWindowedSlug_FullSlugAccepted(t *testing.T) {
	ws, err := parseWindowedSlug("btc-updown-15m-1765791900")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.stem != "btc-updown-15m" {
		t.Fatalf("unexpected stem: %q", ws.stem)
	}
	if ws.interval != 15*time.Minute {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 0 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}
}

func TestParseWindowedSlug_WithOffset(t *testing.T) {
	ws, err := parseWindowedSlug("btc-updown-15m+1")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.stem != "btc-updown-15m" {
		t.Fatalf("unexpected stem: %q", ws.stem)
	}
	if ws.interval != 15*time.Minute {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 1 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}
}

func TestParseWindowedSlug_HourlyETFromFullSlug(t *testing.T) {
	ws, err := parseWindowedSlug("bitcoin-up-or-down-december-15-3pm-et")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.kind != windowedSlugKindHourlyEtHumanSuffix {
		t.Fatalf("unexpected kind: %v", ws.kind)
	}
	if ws.stem != "bitcoin-up-or-down" {
		t.Fatalf("unexpected stem: %q", ws.stem)
	}
	if ws.interval != time.Hour {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 0 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	startET := time.Date(2025, 12, 15, 15, 0, 0, 0, loc)
	if got, want := ws.slugForWindowStart(startET), "bitcoin-up-or-down-december-15-3pm-et"; got != want {
		t.Fatalf("slugForWindowStart: got %q want %q", got, want)
	}
}

func TestParseWindowedSlug_HourlyETFromStem(t *testing.T) {
	ws, err := parseWindowedSlug("bitcoin-up-or-down")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.kind != windowedSlugKindHourlyEtHumanSuffix {
		t.Fatalf("unexpected kind: %v", ws.kind)
	}
	if ws.interval != time.Hour {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 0 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}
}

func TestParseWindowedSlug_HourlyETWithOffset(t *testing.T) {
	ws, err := parseWindowedSlug("bitcoin-up-or-down+1")
	if err != nil {
		t.Fatalf("parseWindowedSlug: %v", err)
	}
	if ws.kind != windowedSlugKindHourlyEtHumanSuffix {
		t.Fatalf("unexpected kind: %v", ws.kind)
	}
	if ws.stem != "bitcoin-up-or-down" {
		t.Fatalf("unexpected stem: %q", ws.stem)
	}
	if ws.interval != time.Hour {
		t.Fatalf("unexpected interval: %s", ws.interval)
	}
	if ws.offset != 1 {
		t.Fatalf("unexpected offset: %d", ws.offset)
	}
}

func TestFloorToIntervalInLocation_ET(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}

	// 2025-12-15 04:52:33 ET should floor to 04:45:00 ET for a 15m window.
	nowET := time.Date(2025, 12, 15, 4, 52, 33, 0, loc)
	start := floorToIntervalInLocation(nowET, 15*time.Minute, loc)

	if start.In(loc).Hour() != 4 || start.In(loc).Minute() != 45 || start.In(loc).Second() != 0 {
		t.Fatalf("unexpected floored time: %s", start.In(loc).Format(time.RFC3339))
	}

	// 2025-12-15 04:45:00 ET == 2025-12-15 09:45:00 UTC == 1765791900.
	if start.Unix() != 1765791900 {
		t.Fatalf("unexpected unix: got %d want %d", start.Unix(), int64(1765791900))
	}
}
