package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"poly-gocopy/internal/dotenv"
)

const (
	defaultCLOBURL = "https://clob.polymarket.com"
	defaultRTDSURL = "wss://ws-live-data.polymarket.com"
)

type stats struct {
	min    int64
	median int64
	p95    int64
	max    int64
}

func summarize(values []int64) stats {
	if len(values) == 0 {
		return stats{}
	}
	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(q float64) int64 {
		idx := int(q * float64(len(sorted)))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return stats{
		min:    sorted[0],
		median: pick(0.5),
		p95:    pick(0.95),
		max:    sorted[len(sorted)-1],
	}
}

type ring struct {
	mu         sync.Mutex
	buf        []int64
	next       int
	hasWrapped bool
}

func newRing(capacity int) *ring {
	if capacity <= 0 {
		capacity = 4096
	}
	return &ring{buf: make([]int64, 0, capacity)}
}

func (r *ring) add(v int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) < cap(r.buf) {
		r.buf = append(r.buf, v)
		return
	}
	r.hasWrapped = true
	r.buf[r.next] = v
	r.next++
	if r.next >= len(r.buf) {
		r.next = 0
	}
}

func (r *ring) snapshot() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		return nil
	}
	out := make([]int64, 0, len(r.buf))
	if !r.hasWrapped || r.next == 0 {
		out = append(out, r.buf...)
		return out
	}
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}

type sampleWriter struct {
	mu   sync.Mutex
	path string
	file *os.File
	w    *bufio.Writer
}

func newSampleWriter(path string) *sampleWriter {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return &sampleWriter{path: path}
}

func (sw *sampleWriter) ensureOpen() error {
	if sw.file != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(sw.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(sw.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	sw.file = f
	sw.w = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (sw *sampleWriter) writeRow(row map[string]any) {
	if sw == nil {
		return
	}
	b, err := json.Marshal(row)
	if err != nil {
		log.Printf("[warn] failed to marshal row: %v", err)
		return
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if err := sw.ensureOpen(); err != nil {
		log.Printf("[warn] failed to open out file %s: %v", sw.path, err)
		return
	}
	if _, err := sw.w.Write(append(b, '\n')); err != nil {
		log.Printf("[warn] failed to write sample to %s: %v", sw.path, err)
		return
	}
	_ = sw.w.Flush()
}

func (sw *sampleWriter) close() {
	if sw == nil {
		return
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.w != nil {
		_ = sw.w.Flush()
	}
	if sw.file != nil {
		_ = sw.file.Close()
	}
}

type args struct {
	label       string
	outFile     string
	duration    time.Duration
	printEvery  time.Duration
	printFormat string

	clobURL              string
	clobTimeout          time.Duration
	clobDisableKeepAlive bool
	clobTimeInterval     time.Duration
	clobBookInterval     time.Duration
	clobTokenID          string

	rtdsURL          string
	rtdsPingInterval time.Duration
	rtdsReadTimeout  time.Duration
	rtdsTopic        string
	rtdsType         string
	rtdsFiltersCSV   string
	rtdsNoSubscribe  bool

	sampleCap int
}

func parseArgs() (args, error) {
	var labelFlag string
	var outFlag string
	var durationFlag time.Duration
	var printEveryFlag time.Duration
	var printFormatFlag string

	var clobURLFlag string
	var clobTimeoutFlag time.Duration
	var clobDisableKeepAliveFlag bool
	var clobTimeIntervalFlag time.Duration
	var clobBookIntervalFlag time.Duration
	var clobTokenIDFlag string

	var rtdsURLFlag string
	var rtdsPingFlag time.Duration
	var rtdsReadTimeoutFlag time.Duration
	var rtdsTopicFlag string
	var rtdsTypeFlag string
	var rtdsFiltersFlag string
	var rtdsNoSubFlag bool

	var sampleCapFlag int

	flag.StringVar(&labelFlag, "label", "", "Optional label for this run (e.g. vpn-nyc, vpn-sg)")
	flag.StringVar(&outFlag, "out", "", "Optional output file path (JSONL)")
	flag.StringVar(&outFlag, "outfile", "", "Optional output file path (JSONL) (alias)")
	flag.DurationVar(&durationFlag, "duration", 30*time.Second, "Total runtime (0 = run until Ctrl+C)")
	flag.DurationVar(&printEveryFlag, "print-every", 5*time.Second, "How often to print summary stats")
	flag.StringVar(&printFormatFlag, "print-format", "table", "Summary output format: table (default) or compact")

	flag.StringVar(&clobURLFlag, "clob-url", "", "CLOB API base URL (default https://clob.polymarket.com)")
	flag.DurationVar(&clobTimeoutFlag, "clob-timeout", 5*time.Second, "Per-request timeout for CLOB HTTP probes")
	flag.BoolVar(&clobDisableKeepAliveFlag, "clob-no-keepalive", false, "Disable HTTP keep-alives (forces new TCP/TLS each request)")
	flag.DurationVar(&clobTimeIntervalFlag, "clob-time-interval", 500*time.Millisecond, "Interval for GET /time probes")
	flag.DurationVar(&clobBookIntervalFlag, "clob-book-interval", 2*time.Second, "Interval for GET /book probes (only if --token-id is set)")
	flag.StringVar(&clobTokenIDFlag, "token-id", "", "Optional token_id to probe GET /book?token_id=... (enables /book latency stats)")

	flag.StringVar(&rtdsURLFlag, "rtds-url", defaultRTDSURL, "RTDS WebSocket URL (default wss://ws-live-data.polymarket.com)")
	flag.DurationVar(&rtdsPingFlag, "rtds-ping", 5*time.Second, "RTDS ping interval (sends text message \"ping\", measures pong RTT)")
	flag.DurationVar(&rtdsReadTimeoutFlag, "rtds-read-timeout", 20*time.Second, "WebSocket read timeout (deadline extended on any message)")
	flag.StringVar(&rtdsTopicFlag, "rtds-topic", "clob_market", "RTDS subscription topic")
	flag.StringVar(&rtdsTypeFlag, "rtds-type", "agg_orderbook", "RTDS subscription type")
	flag.StringVar(&rtdsFiltersFlag, "rtds-filters", "", "Comma-separated token IDs for RTDS subscription filters (e.g. 123,456). Required unless --rtds-no-subscribe.")
	flag.BoolVar(&rtdsNoSubFlag, "rtds-no-subscribe", false, "Only do RTDS ping/pong (skip subscribe/message-latency)")

	flag.IntVar(&sampleCapFlag, "sample-cap", 4096, "Max samples kept per metric in memory (ring buffer)")

	flag.Parse()

	label := strings.TrimSpace(labelFlag)
	if label == "" {
		label = strings.TrimSpace(os.Getenv("LATENCY_LABEL"))
	}

	outFile := strings.TrimSpace(outFlag)
	if outFile == "" {
		outFile = strings.TrimSpace(os.Getenv("LATENCY_OUT_FILE"))
	}

	if printEveryFlag <= 0 {
		return args{}, fmt.Errorf("print-every must be > 0")
	}
	if durationFlag < 0 {
		return args{}, fmt.Errorf("duration must be >= 0")
	}
	printFormat := strings.ToLower(strings.TrimSpace(printFormatFlag))
	if printFormat == "" {
		printFormat = "table"
	}
	switch printFormat {
	case "table", "compact":
	default:
		return args{}, fmt.Errorf("invalid print-format %q (use table or compact)", printFormatFlag)
	}

	clobURL := strings.TrimSpace(clobURLFlag)
	if clobURL == "" {
		clobURL = strings.TrimSpace(os.Getenv("CLOB_URL"))
	}
	if clobURL == "" {
		clobURL = defaultCLOBURL
	}
	clobURL = strings.TrimRight(clobURL, "/")
	if !strings.HasPrefix(clobURL, "http://") && !strings.HasPrefix(clobURL, "https://") {
		return args{}, fmt.Errorf("clob-url must start with http:// or https:// (got %q)", clobURL)
	}
	if clobTimeoutFlag <= 0 {
		return args{}, fmt.Errorf("clob-timeout must be > 0")
	}
	if clobTimeIntervalFlag <= 0 {
		return args{}, fmt.Errorf("clob-time-interval must be > 0")
	}
	if clobBookIntervalFlag <= 0 {
		return args{}, fmt.Errorf("clob-book-interval must be > 0")
	}
	clobTokenID := strings.TrimSpace(clobTokenIDFlag)
	if clobTokenID == "" {
		clobTokenID = strings.TrimSpace(os.Getenv("CLOB_TOKEN_ID"))
	}

	rtdsURL := strings.TrimSpace(rtdsURLFlag)
	if rtdsURL == "" {
		rtdsURL = strings.TrimSpace(os.Getenv("RTDS_URL"))
	}
	if rtdsURL == "" {
		rtdsURL = defaultRTDSURL
	}
	if !strings.HasPrefix(rtdsURL, "ws://") && !strings.HasPrefix(rtdsURL, "wss://") {
		return args{}, fmt.Errorf("rtds-url must start with ws:// or wss:// (got %q)", rtdsURL)
	}
	if rtdsPingFlag <= 0 {
		return args{}, fmt.Errorf("rtds-ping must be > 0")
	}
	if rtdsReadTimeoutFlag <= 0 {
		return args{}, fmt.Errorf("rtds-read-timeout must be > 0")
	}
	rtdsTopic := strings.TrimSpace(rtdsTopicFlag)
	rtdsType := strings.TrimSpace(rtdsTypeFlag)
	if rtdsTopic == "" || rtdsType == "" {
		return args{}, fmt.Errorf("rtds-topic and rtds-type must be non-empty")
	}
	rtdsFiltersCSV := strings.TrimSpace(rtdsFiltersFlag)
	if rtdsFiltersCSV == "" {
		rtdsFiltersCSV = strings.TrimSpace(os.Getenv("RTDS_FILTERS"))
	}
	if !rtdsNoSubFlag && rtdsFiltersCSV == "" {
		return args{}, fmt.Errorf("rtds-filters is required unless --rtds-no-subscribe is set")
	}
	if sampleCapFlag <= 0 {
		return args{}, fmt.Errorf("sample-cap must be > 0")
	}

	return args{
		label:                label,
		outFile:              outFile,
		duration:             durationFlag,
		printEvery:           printEveryFlag,
		printFormat:          printFormat,
		clobURL:              clobURL,
		clobTimeout:          clobTimeoutFlag,
		clobDisableKeepAlive: clobDisableKeepAliveFlag,
		clobTimeInterval:     clobTimeIntervalFlag,
		clobBookInterval:     clobBookIntervalFlag,
		clobTokenID:          clobTokenID,
		rtdsURL:              rtdsURL,
		rtdsPingInterval:     rtdsPingFlag,
		rtdsReadTimeout:      rtdsReadTimeoutFlag,
		rtdsTopic:            rtdsTopic,
		rtdsType:             rtdsType,
		rtdsFiltersCSV:       rtdsFiltersCSV,
		rtdsNoSubscribe:      rtdsNoSubFlag,
		sampleCap:            sampleCapFlag,
	}, nil
}

type httpMetrics struct {
	totalMs  *ring
	ttfbMs   *ring
	dnsMs    *ring
	connMs   *ring
	tlsMs    *ring
	offsetMs *ring

	ok     atomic.Int64
	errors atomic.Int64
	reused atomic.Int64
}

func newHTTPMetrics(sampleCap int) *httpMetrics {
	return &httpMetrics{
		totalMs:  newRing(sampleCap),
		ttfbMs:   newRing(sampleCap),
		dnsMs:    newRing(sampleCap),
		connMs:   newRing(sampleCap),
		tlsMs:    newRing(sampleCap),
		offsetMs: newRing(sampleCap),
	}
}

type wsMetrics struct {
	dialMs *ring

	pingRttMs *ring
	pingLost  atomic.Int64

	msgDelayRawMs *ring
	msgDelayAdjMs *ring
	msgDecodeErr  atomic.Int64
	msgCount      atomic.Int64

	firstDataAfterSubMs *ring
}

func newWSMetrics(sampleCap int) *wsMetrics {
	return &wsMetrics{
		dialMs:              newRing(sampleCap),
		pingRttMs:           newRing(sampleCap),
		msgDelayRawMs:       newRing(sampleCap),
		msgDelayAdjMs:       newRing(sampleCap),
		firstDataAfterSubMs: newRing(sampleCap),
	}
}

type rtdsEnvelope struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func unixFromUnknown(ts int64) time.Time {
	// Heuristic based on magnitude:
	// - seconds: ~1e9
	// - millis:  ~1e12
	// - micros:  ~1e15
	// - nanos:   ~1e18
	switch {
	case ts > 1e18:
		return time.Unix(0, ts)
	case ts > 1e15:
		return time.Unix(0, ts*int64(time.Microsecond))
	case ts > 1e12:
		return time.Unix(0, ts*int64(time.Millisecond))
	default:
		return time.Unix(ts, 0)
	}
}

type traceTimings struct {
	dns        time.Duration
	connect    time.Duration
	tls        time.Duration
	ttfb       time.Duration
	connReused bool
}

func httpGetTimed(ctx context.Context, client *http.Client, fullURL string, maxBodyBytes int64) (traceTimings, int, []byte, error) {
	var (
		t0 = time.Now()
		td traceTimings

		dnsStart, connStart, tlsStart time.Time
		gotFirstByte                  time.Time
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone: func(httptrace.DNSDoneInfo) {
			if !dnsStart.IsZero() {
				td.dns = time.Since(dnsStart)
			}
		},
		ConnectStart: func(_, _ string) { connStart = time.Now() },
		ConnectDone: func(_, _ string, err error) {
			if err == nil && !connStart.IsZero() {
				td.connect = time.Since(connStart)
			}
		},
		TLSHandshakeStart: func() { tlsStart = time.Now() },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err == nil && !tlsStart.IsZero() {
				td.tls = time.Since(tlsStart)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) { td.connReused = info.Reused },
		GotFirstResponseByte: func() {
			if gotFirstByte.IsZero() {
				gotFirstByte = time.Now()
			}
		},
	}

	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, fullURL, nil)
	if err != nil {
		return traceTimings{}, 0, nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return traceTimings{}, 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if gotFirstByte.IsZero() {
		gotFirstByte = time.Now()
	}
	td.ttfb = gotFirstByte.Sub(t0)
	if err != nil {
		return td, resp.StatusCode, body, err
	}
	return td, resp.StatusCode, body, nil
}

func fmtMs(ms int64) string {
	sign := ""
	if ms < 0 {
		sign = "-"
		ms = -ms
	}
	switch {
	case ms >= 60_000:
		return fmt.Sprintf("%s%.1fm", sign, float64(ms)/60_000.0)
	case ms >= 1_000:
		return fmt.Sprintf("%s%.2fs", sign, float64(ms)/1_000.0)
	default:
		return fmt.Sprintf("%s%dms", sign, ms)
	}
}

func clobTimeLoop(
	ctx context.Context,
	baseURL string,
	interval time.Duration,
	timeout time.Duration,
	client *http.Client,
	metrics *httpMetrics,
	offsetEstimateMs *atomic.Int64,
	label string,
	writer *sampleWriter,
) {
	t := time.NewTicker(interval)
	defer t.Stop()

	run := func() {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		t0 := time.Now()
		td, status, body, err := httpGetTimed(reqCtx, client, baseURL+"/time", 64*1024)
		t1 := time.Now()
		totalMs := t1.Sub(t0).Milliseconds()
		ttfbMs := td.ttfb.Milliseconds()
		dnsMs := td.dns.Milliseconds()
		connMs := td.connect.Milliseconds()
		tlsMs := td.tls.Milliseconds()

		row := map[string]any{
			"ts":        time.Now().UTC().Format(time.RFC3339Nano),
			"label":     label,
			"metric":    "clob_time",
			"url":       baseURL + "/time",
			"status":    status,
			"total_ms":  totalMs,
			"ttfb_ms":   ttfbMs,
			"dns_ms":    dnsMs,
			"conn_ms":   connMs,
			"tls_ms":    tlsMs,
			"reused":    td.connReused,
			"err":       "",
			"server_ts": nil,
		}

		if td.connReused {
			metrics.reused.Add(1)
		}

		if err != nil || status < 200 || status >= 300 {
			metrics.errors.Add(1)
			if err != nil {
				row["err"] = err.Error()
			} else {
				row["err"] = strings.TrimSpace(string(body))
			}
			writer.writeRow(row)
			return
		}

		var serverTS int64
		if derr := json.Unmarshal(body, &serverTS); derr != nil {
			metrics.errors.Add(1)
			row["err"] = fmt.Sprintf("decode /time: %v", derr)
			writer.writeRow(row)
			return
		}

		serverTime := unixFromUnknown(serverTS)
		localMid := t0.Add(t1.Sub(t0) / 2)
		offset := serverTime.Sub(localMid)

		metrics.ok.Add(1)
		metrics.totalMs.add(totalMs)
		metrics.ttfbMs.add(ttfbMs)
		metrics.dnsMs.add(dnsMs)
		metrics.connMs.add(connMs)
		metrics.tlsMs.add(tlsMs)
		metrics.offsetMs.add(offset.Milliseconds())
		offsetEstimateMs.Store(offset.Milliseconds())

		row["server_ts"] = serverTS
		row["offset_ms"] = offset.Milliseconds()
		writer.writeRow(row)
	}

	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

func clobBookLoop(
	ctx context.Context,
	baseURL string,
	tokenID string,
	interval time.Duration,
	timeout time.Duration,
	client *http.Client,
	metrics *httpMetrics,
	label string,
	writer *sampleWriter,
) {
	if tokenID == "" {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	bookURL := baseURL + "/book?token_id=" + url.QueryEscape(tokenID)

	run := func() {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		t0 := time.Now()
		td, status, body, err := httpGetTimed(reqCtx, client, bookURL, 2<<20)
		t1 := time.Now()
		totalMs := t1.Sub(t0).Milliseconds()
		ttfbMs := td.ttfb.Milliseconds()
		dnsMs := td.dns.Milliseconds()
		connMs := td.connect.Milliseconds()
		tlsMs := td.tls.Milliseconds()

		row := map[string]any{
			"ts":       time.Now().UTC().Format(time.RFC3339Nano),
			"label":    label,
			"metric":   "clob_book",
			"url":      bookURL,
			"token_id": tokenID,
			"status":   status,
			"total_ms": totalMs,
			"ttfb_ms":  ttfbMs,
			"dns_ms":   dnsMs,
			"conn_ms":  connMs,
			"tls_ms":   tlsMs,
			"reused":   td.connReused,
			"err":      "",
		}

		if td.connReused {
			metrics.reused.Add(1)
		}

		if err != nil || status < 200 || status >= 300 {
			metrics.errors.Add(1)
			if err != nil {
				row["err"] = err.Error()
			} else {
				row["err"] = strings.TrimSpace(string(body))
			}
			writer.writeRow(row)
			return
		}
		if !json.Valid(body) {
			metrics.errors.Add(1)
			row["err"] = "invalid json body"
			writer.writeRow(row)
			return
		}

		metrics.ok.Add(1)
		metrics.totalMs.add(totalMs)
		metrics.ttfbMs.add(ttfbMs)
		metrics.dnsMs.add(dnsMs)
		metrics.connMs.add(connMs)
		metrics.tlsMs.add(tlsMs)
		writer.writeRow(row)
	}

	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

func parseCSVTokens(csv string) ([]string, error) {
	raw := strings.TrimSpace(csv)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tokens found in csv %q", csv)
	}
	return out, nil
}

func rtdsLoop(
	ctx context.Context,
	wsURL string,
	pingInterval time.Duration,
	readTimeout time.Duration,
	subTopic string,
	subType string,
	filtersCSV string,
	noSubscribe bool,
	offsetEstimateMs *atomic.Int64,
	metrics *wsMetrics,
	label string,
	writer *sampleWriter,
) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}

	dialStart := time.Now()
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	dialDur := time.Since(dialStart)
	metrics.dialMs.add(dialDur.Milliseconds())

	if err != nil {
		writer.writeRow(map[string]any{
			"ts":       time.Now().UTC().Format(time.RFC3339Nano),
			"label":    label,
			"metric":   "rtds_dial",
			"url":      wsURL,
			"total_ms": dialDur.Milliseconds(),
			"status":   0,
			"err":      err.Error(),
		})
		return fmt.Errorf("rtds dial: %w", err)
	}
	defer conn.Close()

	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	writer.writeRow(map[string]any{
		"ts":       time.Now().UTC().Format(time.RFC3339Nano),
		"label":    label,
		"metric":   "rtds_dial",
		"url":      wsURL,
		"total_ms": dialDur.Milliseconds(),
		"status":   status,
		"err":      "",
	})

	conn.SetReadLimit(32 << 20)

	var writeMu sync.Mutex
	pendingPingSentNs := atomic.Int64{}

	subSentAt := time.Time{}
	var firstDataOnce sync.Once

	if !noSubscribe {
		tokens, err := parseCSVTokens(filtersCSV)
		if err != nil {
			return err
		}
		filtersBytes, err := json.Marshal(tokens)
		if err != nil {
			return fmt.Errorf("marshal rtds filters: %w", err)
		}

		req := map[string]any{
			"action": "subscribe",
			"subscriptions": []map[string]any{
				{
					"topic":   subTopic,
					"type":    subType,
					"filters": string(filtersBytes),
				},
			},
		}
		reqBytes, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("marshal rtds subscribe: %w", err)
		}

		writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		werr := conn.WriteMessage(websocket.TextMessage, reqBytes)
		writeMu.Unlock()
		if werr != nil {
			return fmt.Errorf("rtds subscribe write: %w", werr)
		}
		subSentAt = time.Now()

		writer.writeRow(map[string]any{
			"ts":      time.Now().UTC().Format(time.RFC3339Nano),
			"label":   label,
			"metric":  "rtds_subscribe",
			"url":     wsURL,
			"topic":   subTopic,
			"type":    subType,
			"filters": tokens,
			"err":     "",
		})
	}

	// Close the socket on context cancel to unblock ReadMessage().
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	// Ping loop: RTDS uses a text "ping" message and replies with text "pong".
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if pendingPingSentNs.Load() != 0 {
					continue
				}
				now := time.Now()
				pendingPingSentNs.Store(now.UnixNano())
				writeMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				err := conn.WriteMessage(websocket.TextMessage, []byte("ping"))
				writeMu.Unlock()
				if err != nil {
					pendingPingSentNs.Store(0)
					metrics.pingLost.Add(1)
					writer.writeRow(map[string]any{
						"ts":     time.Now().UTC().Format(time.RFC3339Nano),
						"label":  label,
						"metric": "rtds_ping_write_err",
						"url":    wsURL,
						"err":    err.Error(),
					})
					_ = conn.Close()
					return
				}
			}
		}
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		typ, msg, err := conn.ReadMessage()
		recvAt := time.Now()
		if err != nil {
			return fmt.Errorf("rtds read: %w", err)
		}
		if typ != websocket.TextMessage && typ != websocket.BinaryMessage {
			continue
		}
		if len(msg) == 0 {
			continue
		}

		if string(msg) == "pong" {
			sent := pendingPingSentNs.Swap(0)
			if sent != 0 {
				rtt := recvAt.Sub(time.Unix(0, sent))
				metrics.pingRttMs.add(rtt.Milliseconds())
				writer.writeRow(map[string]any{
					"ts":     time.Now().UTC().Format(time.RFC3339Nano),
					"label":  label,
					"metric": "rtds_pong_rtt",
					"url":    wsURL,
					"rtt_ms": rtt.Milliseconds(),
				})
			}
			continue
		}
		if string(msg) == "ping" {
			continue
		}

		var env rtdsEnvelope
		if err := json.Unmarshal(msg, &env); err != nil {
			metrics.msgDecodeErr.Add(1)
			writer.writeRow(map[string]any{
				"ts":     time.Now().UTC().Format(time.RFC3339Nano),
				"label":  label,
				"metric": "rtds_msg_decode_err",
				"url":    wsURL,
				"err":    err.Error(),
			})
			continue
		}

		metrics.msgCount.Add(1)

		if !noSubscribe && env.Topic == subTopic && env.Type == subType {
			firstDataOnce.Do(func() {
				if !subSentAt.IsZero() {
					metrics.firstDataAfterSubMs.add(recvAt.Sub(subSentAt).Milliseconds())
					writer.writeRow(map[string]any{
						"ts":            time.Now().UTC().Format(time.RFC3339Nano),
						"label":         label,
						"metric":        "rtds_first_data_after_sub",
						"url":           wsURL,
						"topic":         subTopic,
						"type":          subType,
						"first_data_ms": recvAt.Sub(subSentAt).Milliseconds(),
					})
				}
			})

			if env.Timestamp > 0 {
				msgTime := unixFromUnknown(env.Timestamp)
				rawDelay := recvAt.Sub(msgTime)
				metrics.msgDelayRawMs.add(rawDelay.Milliseconds())

				off := time.Duration(offsetEstimateMs.Load()) * time.Millisecond
				if off != 0 && off > -24*time.Hour && off < 24*time.Hour {
					adjDelay := recvAt.Add(off).Sub(msgTime)
					metrics.msgDelayAdjMs.add(adjDelay.Milliseconds())
					writer.writeRow(map[string]any{
						"ts":            time.Now().UTC().Format(time.RFC3339Nano),
						"label":         label,
						"metric":        "rtds_msg_delay",
						"url":           wsURL,
						"topic":         subTopic,
						"type":          subType,
						"server_ts":     env.Timestamp,
						"delay_raw_ms":  rawDelay.Milliseconds(),
						"delay_adj_ms":  adjDelay.Milliseconds(),
						"offset_est_ms": off.Milliseconds(),
					})
				} else {
					writer.writeRow(map[string]any{
						"ts":           time.Now().UTC().Format(time.RFC3339Nano),
						"label":        label,
						"metric":       "rtds_msg_delay",
						"url":          wsURL,
						"topic":        subTopic,
						"type":         subType,
						"server_ts":    env.Timestamp,
						"delay_raw_ms": rawDelay.Milliseconds(),
					})
				}
			}
		}
	}
}

func fmtStatTriplet(st stats, n int) string {
	if n <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("p50=%s p95=%s max=%s", fmtMs(st.median), fmtMs(st.p95), fmtMs(st.max))
}

func printHTTPStatsCompact(prefix string, m *httpMetrics, includeOffset bool) {
	total := m.totalMs.snapshot()
	ttfb := m.ttfbMs.snapshot()
	dns := m.dnsMs.snapshot()
	conn := m.connMs.snapshot()
	tls := m.tlsMs.snapshot()
	var offset []int64
	if includeOffset {
		offset = m.offsetMs.snapshot()
	}

	ok := m.ok.Load()
	errs := m.errors.Load()
	reused := m.reused.Load()

	totalReq := ok + errs
	reusedPct := int64(0)
	if totalReq > 0 {
		reusedPct = (100 * reused) / totalReq
	}

	stTotal := summarize(total)
	stTTFB := summarize(ttfb)
	stDNS := summarize(dns)
	stConn := summarize(conn)
	stTLS := summarize(tls)
	stOff := summarize(offset)

	if includeOffset {
		log.Printf("%s: ok=%d err=%d reused=%d (%d%%) total(p50/p95/max)=%s/%s/%s ttfb(p50)=%s dns(p50)=%s conn(p50)=%s tls(p50)=%s offset(p50)=%s",
			prefix,
			ok, errs,
			reused, reusedPct,
			fmtMs(stTotal.median), fmtMs(stTotal.p95), fmtMs(stTotal.max),
			fmtMs(stTTFB.median), fmtMs(stDNS.median), fmtMs(stConn.median), fmtMs(stTLS.median),
			fmtMs(stOff.median),
		)
		return
	}

	log.Printf("%s: ok=%d err=%d reused=%d (%d%%) total(p50/p95/max)=%s/%s/%s ttfb(p50)=%s dns(p50)=%s conn(p50)=%s tls(p50)=%s",
		prefix,
		ok, errs,
		reused, reusedPct,
		fmtMs(stTotal.median), fmtMs(stTotal.p95), fmtMs(stTotal.max),
		fmtMs(stTTFB.median), fmtMs(stDNS.median), fmtMs(stConn.median), fmtMs(stTLS.median),
	)
}

func printHTTPStatsTable(prefix string, m *httpMetrics, includeOffset bool) {
	total := m.totalMs.snapshot()
	ttfb := m.ttfbMs.snapshot()
	dns := m.dnsMs.snapshot()
	conn := m.connMs.snapshot()
	tls := m.tlsMs.snapshot()
	var offset []int64
	if includeOffset {
		offset = m.offsetMs.snapshot()
	}

	ok := m.ok.Load()
	errs := m.errors.Load()
	reused := m.reused.Load()
	totalReq := ok + errs
	reusedPct := int64(0)
	if totalReq > 0 {
		reusedPct = (100 * reused) / totalReq
	}

	stTotal := summarize(total)
	stTTFB := summarize(ttfb)
	stDNS := summarize(dns)
	stConn := summarize(conn)
	stTLS := summarize(tls)
	stOff := summarize(offset)

	log.Printf("%-10s  ok=%-6d err=%-6d reused=%-6d (%d%%) samples=%d", prefix, ok, errs, reused, reusedPct, len(total))
	log.Printf("  total:  %s", fmtStatTriplet(stTotal, len(total)))
	log.Printf("  ttfb:   p50=%-8s  dns: p50=%-8s  conn: p50=%-8s  tls: p50=%-8s", fmtMs(stTTFB.median), fmtMs(stDNS.median), fmtMs(stConn.median), fmtMs(stTLS.median))
	if includeOffset {
		log.Printf("  offset: p50=%s (server - local_midpoint)", fmtMs(stOff.median))
	}
}

func printWSStatsCompact(prefix string, m *wsMetrics) {
	dial := m.dialMs.snapshot()
	ping := m.pingRttMs.snapshot()
	raw := m.msgDelayRawMs.snapshot()
	adj := m.msgDelayAdjMs.snapshot()
	first := m.firstDataAfterSubMs.snapshot()

	stDial := summarize(dial)
	stPing := summarize(ping)
	stRaw := summarize(raw)
	stAdj := summarize(adj)
	stFirst := summarize(first)

	log.Printf("%s: dial(p50/p95/max)=%s/%s/%s ping_rtt(p50/p95/max)=%s/%s/%s ping_lost=%d msgs=%d decode_err=%d first_data(p50)=%s msg_delay_raw(p50/p95/max)=%s/%s/%s msg_delay_adj(p50/p95/max)=%s/%s/%s",
		prefix,
		fmtMs(stDial.median), fmtMs(stDial.p95), fmtMs(stDial.max),
		fmtMs(stPing.median), fmtMs(stPing.p95), fmtMs(stPing.max),
		m.pingLost.Load(),
		m.msgCount.Load(),
		m.msgDecodeErr.Load(),
		fmtMs(stFirst.median),
		fmtMs(stRaw.median), fmtMs(stRaw.p95), fmtMs(stRaw.max),
		fmtMs(stAdj.median), fmtMs(stAdj.p95), fmtMs(stAdj.max),
	)
}

func printWSStatsTable(prefix string, m *wsMetrics) {
	dial := m.dialMs.snapshot()
	ping := m.pingRttMs.snapshot()
	first := m.firstDataAfterSubMs.snapshot()
	raw := m.msgDelayRawMs.snapshot()
	adj := m.msgDelayAdjMs.snapshot()

	stDial := summarize(dial)
	stPing := summarize(ping)
	stFirst := summarize(first)
	stRaw := summarize(raw)
	stAdj := summarize(adj)

	log.Printf("%-10s  msgs=%-7d decode_err=%-5d ping_lost=%-5d", prefix, m.msgCount.Load(), m.msgDecodeErr.Load(), m.pingLost.Load())
	log.Printf("  dial:       %s", fmtStatTriplet(stDial, len(dial)))
	log.Printf("  ping_rtt:   %s", fmtStatTriplet(stPing, len(ping)))
	if len(first) > 0 {
		log.Printf("  first_data: %s", fmtStatTriplet(stFirst, len(first)))
	}
	if len(raw) > 0 {
		log.Printf("  msg_delay:  raw %s", fmtStatTriplet(stRaw, len(raw)))
	}
	if len(adj) > 0 {
		log.Printf("             adj %s", fmtStatTriplet(stAdj, len(adj)))
	}
}

func printSummary(format string, label string, clobTime *httpMetrics, clobBook *httpMetrics, hasBook bool, rtds *wsMetrics) {
	if label != "" {
		log.Printf("=== summary  label=%s ===", label)
	} else {
		log.Printf("=== summary ===")
	}
	switch format {
	case "compact":
		printHTTPStatsCompact("CLOB /time", clobTime, true)
		if hasBook {
			printHTTPStatsCompact("CLOB /book", clobBook, false)
		}
		printWSStatsCompact("RTDS", rtds)
	default:
		printHTTPStatsTable("CLOB /time", clobTime, true)
		if hasBook {
			printHTTPStatsTable("CLOB /book", clobBook, false)
		}
		printWSStatsTable("RTDS", rtds)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	log.SetOutput(os.Stdout)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	parsed, err := parseArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	baseCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx := baseCtx
	if parsed.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(baseCtx, parsed.duration)
		defer cancel()
	}

	writer := newSampleWriter(parsed.outFile)
	defer writer.close()

	log.Printf("Latency probe starting")
	log.Printf("Label: %q", parsed.label)
	log.Printf("Duration: %s (Ctrl+C to stop early)", parsed.duration)
	log.Printf("CLOB: %s (/time every %s, /book every %s token_id=%q)", parsed.clobURL, parsed.clobTimeInterval, parsed.clobBookInterval, parsed.clobTokenID)
	if parsed.rtdsNoSubscribe {
		log.Printf("RTDS: %s (ping every %s, no subscribe)", parsed.rtdsURL, parsed.rtdsPingInterval)
	} else {
		log.Printf("RTDS: %s (ping every %s, subscribe topic=%s type=%s filters=%q)", parsed.rtdsURL, parsed.rtdsPingInterval, parsed.rtdsTopic, parsed.rtdsType, parsed.rtdsFiltersCSV)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = parsed.clobDisableKeepAlive
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 30 * time.Second

	httpClient := &http.Client{Transport: transport}

	var offsetEstimateMs atomic.Int64
	clobTimeMetrics := newHTTPMetrics(parsed.sampleCap)
	clobBookMetrics := newHTTPMetrics(parsed.sampleCap)
	rtdsMetrics := newWSMetrics(parsed.sampleCap)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		clobTimeLoop(ctx, parsed.clobURL, parsed.clobTimeInterval, parsed.clobTimeout, httpClient, clobTimeMetrics, &offsetEstimateMs, parsed.label, writer)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		clobBookLoop(ctx, parsed.clobURL, parsed.clobTokenID, parsed.clobBookInterval, parsed.clobTimeout, httpClient, clobBookMetrics, parsed.label, writer)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rtdsLoop(ctx, parsed.rtdsURL, parsed.rtdsPingInterval, parsed.rtdsReadTimeout, parsed.rtdsTopic, parsed.rtdsType, parsed.rtdsFiltersCSV, parsed.rtdsNoSubscribe, &offsetEstimateMs, rtdsMetrics, parsed.label, writer); err != nil && ctx.Err() == nil {
			log.Printf("[warn] %v", err)
		}
	}()

	printTicker := time.NewTicker(parsed.printEvery)
	defer printTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			log.Printf("Final summary:")
			printSummary(parsed.printFormat, parsed.label, clobTimeMetrics, clobBookMetrics, parsed.clobTokenID != "", rtdsMetrics)
			return
		case <-printTicker.C:
			printSummary(parsed.printFormat, parsed.label, clobTimeMetrics, clobBookMetrics, parsed.clobTokenID != "", rtdsMetrics)
		}
	}
}
