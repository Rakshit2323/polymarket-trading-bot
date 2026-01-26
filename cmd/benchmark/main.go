package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/ethutil"
)

type txTiming struct {
	txHash   common.Hash
	leader   common.Address
	roles    uint8 // bitmask: 1=maker, 2=taker
	blockNum uint64
	tLogs    int64
	hasLogs  bool
}

const (
	roleMaker uint8 = 1 << iota
	roleTaker
)

type timingKey struct {
	txHash common.Hash
	leader common.Address
}

type timingStore struct {
	mu   sync.RWMutex
	data map[timingKey]*txTiming
}

func newTimingStore() *timingStore {
	return &timingStore{data: make(map[timingKey]*txTiming)}
}

func (s *timingStore) upsertLogs(hash common.Hash, leader common.Address, roles uint8, blockNum uint64, t int64) {
	key := timingKey{txHash: hash, leader: leader}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok := s.data[key]
	if !ok {
		prev = &txTiming{txHash: hash, leader: leader}
		s.data[key] = prev
	}
	if !prev.hasLogs || t < prev.tLogs {
		prev.tLogs = t
	}
	prev.hasLogs = true
	prev.roles |= roles
	if blockNum != 0 {
		prev.blockNum = blockNum
	}
}

func (s *timingStore) completedLogs() []txTiming {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]txTiming, 0, len(s.data))
	for _, v := range s.data {
		if v.hasLogs {
			out = append(out, *v)
		}
	}
	return out
}

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

type sampleWriter struct {
	mu   sync.Mutex
	path string
	file *os.File
	w    *bufio.Writer
}

func newSampleWriter(path string) *sampleWriter {
	if path == "" {
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
	leaders      []common.Address
	leaderSet    map[common.Address]struct{}
	leaderTopics []common.Hash
	polygonWs    string
	dumpEveryMs  int
	outFile      string
}

func parseArgs() (args, error) {
	var leaderFlag string
	var polygonWsFlag string
	var dumpMsFlag int
	var outFlag string

	flag.StringVar(&leaderFlag, "leader", "", "Leader proxy address(es) 0x... (comma/space-separated; or LEADER_PROXIES/LEADER_PROXY)")
	flag.StringVar(&leaderFlag, "leaders", "", "Leader proxy address(es) 0x... (comma/space-separated) (alias)")
	flag.StringVar(&polygonWsFlag, "polygon-ws", "", "Polygon WebSocket RPC URL (wss://...)")
	flag.IntVar(&dumpMsFlag, "dump-ms", 10000, "Dump interval in ms")
	flag.StringVar(&outFlag, "out", "", "Optional output file path")
	flag.StringVar(&outFlag, "outfile", "", "Optional output file path (alias)")
	flag.Parse()

	leadersRaw := strings.TrimSpace(leaderFlag)
	if leadersRaw == "" {
		leadersRaw = strings.TrimSpace(firstNonEmpty(os.Getenv("LEADER_PROXIES"), os.Getenv("LEADER_PROXY")))
	}
	if leadersRaw == "" {
		return args{}, fmt.Errorf("leader required via --leader/--leaders or LEADER_PROXIES/LEADER_PROXY")
	}
	leaders, err := ethutil.ParseAddressList(leadersRaw)
	if err != nil {
		return args{}, fmt.Errorf("invalid leader list %q: %w", leadersRaw, err)
	}
	if len(leaders) == 0 {
		return args{}, fmt.Errorf("no leaders found in %q", leadersRaw)
	}
	leaderSet := ethutil.AddressSet(leaders)
	leaderTopics := ethutil.AddressesToTopics(leaders)

	envWs := firstNonEmpty(os.Getenv("RPC_WS_URL"), os.Getenv("RPC_URL"), os.Getenv("POLYGON_WS_URL"))
	polygonWs := polygonWsFlag
	if polygonWs == "" {
		polygonWs = envWs
	}
	if polygonWs == "" {
		polygonWs = "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
	}
	if !strings.HasPrefix(polygonWs, "wss") {
		return args{}, fmt.Errorf(`RPC_WS_URL / polygon-ws must be a WebSocket endpoint (got %q). Set RPC_WS_URL in .env to your node's wss URL`, polygonWs)
	}
	if strings.Contains(strings.ToUpper(polygonWs), "YOUR_KEY") {
		return args{}, fmt.Errorf("Polygon WS URL still contains placeholder YOUR_KEY. Set RPC_WS_URL or use --polygon-ws with a real provider key")
	}

	outFile := outFlag
	if outFile == "" {
		outFile = os.Getenv("LATENCY_OUT_FILE")
	}

	return args{
		leaders:      leaders,
		leaderSet:    leaderSet,
		leaderTopics: leaderTopics,
		polygonWs:    polygonWs,
		dumpEveryMs:  dumpMsFlag,
		outFile:      outFile,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

var (
	exchangeAddresses = [...]common.Address{
		common.HexToAddress("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
		common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
	}
	orderFilledSig   = []byte("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
	orderFilledTopic = crypto.Keccak256Hash(orderFilledSig)
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	parsed, err := parseArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	log.Printf("Benchmark OrderFilled Log Latency (RPC logs)")
	if len(parsed.leaders) == 1 {
		log.Printf("Leader proxy: %s", parsed.leaders[0].Hex())
	} else {
		log.Printf("Leader proxies: %s", ethutil.JoinHex(parsed.leaders))
	}

	client, err := ethclient.Dial(parsed.polygonWs)
	if err != nil {
		log.Fatalf("[fatal] failed to dial polygon ws: %v", err)
	}
	defer client.Close()

	store := newTimingStore()
	var blockCache sync.Map // map[uint64]int64 (ms); -1=in-flight

	writer := newSampleWriter(parsed.outFile)
	defer writer.close()

	// Close on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	makerQuery := ethereum.FilterQuery{
		Addresses: exchangeAddresses[:],
		Topics:    [][]common.Hash{{orderFilledTopic}, nil, parsed.leaderTopics},
	}
	takerQuery := ethereum.FilterQuery{
		Addresses: exchangeAddresses[:],
		Topics:    [][]common.Hash{{orderFilledTopic}, nil, nil, parsed.leaderTopics},
	}

	makerLogsCh := make(chan types.Log, 2048)
	makerSub, err := client.SubscribeFilterLogs(context.Background(), makerQuery, makerLogsCh)
	if err != nil {
		log.Fatalf("[fatal] failed to subscribe (maker filter): %v", err)
	}
	takerLogsCh := make(chan types.Log, 2048)
	takerSub, err := client.SubscribeFilterLogs(context.Background(), takerQuery, takerLogsCh)
	if err != nil {
		makerSub.Unsubscribe()
		log.Fatalf("[fatal] failed to subscribe (taker filter): %v", err)
	}
	log.Printf("Subscribed to logs on %s", parsed.polygonWs)

	ticker := time.NewTicker(time.Duration(parsed.dumpEveryMs) * time.Millisecond)
	defer ticker.Stop()

	type logKey struct {
		txHash common.Hash
		index  uint
	}
	const seenMax = 65536
	seen := make(map[logKey]struct{}, seenMax)
	seenRing := make([]logKey, 0, seenMax)
	seenHead := 0

	for {
		select {
		case <-sigCh:
			log.Printf("Shutting down...")
			takerSub.Unsubscribe()
			makerSub.Unsubscribe()
			return

		case err := <-makerSub.Err():
			if err != nil {
				log.Fatalf("[fatal] polygon ws maker subscription error: %v", err)
			}
			return

		case err := <-takerSub.Err():
			if err != nil {
				log.Fatalf("[fatal] polygon ws taker subscription error: %v", err)
			}
			return

		case vLog := <-makerLogsCh:
			key := logKey{txHash: vLog.TxHash, index: vLog.Index}
			if _, ok := seen[key]; ok {
				continue
			}
			if len(seenRing) < seenMax {
				seenRing = append(seenRing, key)
			} else {
				old := seenRing[seenHead]
				delete(seen, old)
				seenRing[seenHead] = key
				seenHead++
				if seenHead >= seenMax {
					seenHead = 0
				}
			}
			seen[key] = struct{}{}
			handleLog(client, &blockCache, store, parsed.leaderSet, vLog)

		case vLog := <-takerLogsCh:
			key := logKey{txHash: vLog.TxHash, index: vLog.Index}
			if _, ok := seen[key]; ok {
				continue
			}
			if len(seenRing) < seenMax {
				seenRing = append(seenRing, key)
			} else {
				old := seenRing[seenHead]
				delete(seen, old)
				seenRing[seenHead] = key
				seenHead++
				if seenHead >= seenMax {
					seenHead = 0
				}
			}
			seen[key] = struct{}{}
			handleLog(client, &blockCache, store, parsed.leaderSet, vLog)

		case <-ticker.C:
			dumpCompleted(&blockCache, store, writer)
		}
	}
}

func handleLog(client *ethclient.Client, blockCache *sync.Map, store *timingStore, leaderSet map[common.Address]struct{}, vLog types.Log) {
	if len(vLog.Topics) < 4 || vLog.Topics[0] != orderFilledTopic {
		return
	}

	maker := common.BytesToAddress(vLog.Topics[2].Bytes())
	taker := common.BytesToAddress(vLog.Topics[3].Bytes())
	_, makerOk := leaderSet[maker]
	_, takerOk := leaderSet[taker]
	if !makerOk && !takerOk {
		return
	}

	txHash := vLog.TxHash
	now := time.Now().UnixMilli()
	if makerOk {
		store.upsertLogs(txHash, maker, roleMaker, vLog.BlockNumber, now)
	}
	if takerOk {
		store.upsertLogs(txHash, taker, roleTaker, vLog.BlockNumber, now)
	}

	blockNum := vLog.BlockNumber
	if blockNum == 0 {
		return
	}
	if _, ok := blockCache.Load(blockNum); ok {
		return
	}

	if _, loaded := blockCache.LoadOrStore(blockNum, int64(-1)); loaded {
		return
	}

	go func(bn uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		blk, err := client.BlockByNumber(ctx, big.NewInt(int64(bn)))
		if err != nil || blk == nil {
			blockCache.Delete(bn)
			if err != nil {
				log.Printf("[warn] block fetch failed for %d: %v", bn, err)
			}
			return
		}
		tsMs := int64(blk.Time()) * 1000
		blockCache.Store(bn, tsMs)
	}(blockNum)

	role := "unknown"
	switch {
	case makerOk && takerOk:
		role = "both"
	case makerOk:
		role = "maker"
	case takerOk:
		role = "taker"
	}
	log.Printf("OrderFilled log for leader (%s) tx=%s block=%d", role, txHash.Hex(), blockNum)
}

func dumpCompleted(blockCache *sync.Map, store *timingStore, writer *sampleWriter) {
	logs := store.completedLogs()
	if len(logs) == 0 {
		log.Printf("No OrderFilled logs yet")
		return
	}

	withBlock := make([]txTiming, 0, len(logs))
	for _, t := range logs {
		if t.blockNum == 0 {
			continue
		}
		if cached, ok := blockCache.Load(t.blockNum); ok {
			if ts, ok := cached.(int64); ok && ts > 0 {
				withBlock = append(withBlock, t)
			}
		}
	}

	if len(withBlock) > 0 {
		deltas := make([]int64, 0, len(withBlock))
		for _, t := range withBlock {
			cached, _ := blockCache.Load(t.blockNum)
			tBlock := cached.(int64)
			deltas = append(deltas, t.tLogs-tBlock)
		}
		st := summarize(deltas)
		log.Printf("Δ(logs-event) ms → n=%d, min %d, median %d, p95 %d, max %d",
			len(deltas), st.min, st.median, st.p95, st.max)
	} else {
		log.Printf("Waiting for block timestamps to compute event→logs latency")
	}

	for _, d := range logs {
		t0 := d.tLogs
		var (
			tBlock   int64
			hasBlock bool
		)
		if d.blockNum != 0 {
			if cached, ok := blockCache.Load(d.blockNum); ok {
				if ts, ok := cached.(int64); ok && ts > 0 {
					tBlock = ts
					hasBlock = true
					if tBlock < t0 {
						t0 = tBlock
					}
				}
			}
		}

		role := "unknown"
		switch d.roles {
		case roleMaker:
			role = "maker"
		case roleTaker:
			role = "taker"
		case roleMaker | roleTaker:
			role = "both"
		}

		row := map[string]any{
			"tx":     d.txHash.Hex(),
			"leader": d.leader.Hex(),
			"role":   role,
			"t_logs": d.tLogs,
		}
		row["ms_from_first_logs"] = d.tLogs - t0
		if hasBlock {
			row["t_block"] = tBlock
			row["ms_event_to_logs"] = d.tLogs - tBlock
		}

		b, _ := json.Marshal(row)
		log.Printf(string(b))
		writer.writeRow(row)
	}
}
