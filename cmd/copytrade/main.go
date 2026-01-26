package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/copytrade"
	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/ethutil"
	"poly-gocopy/internal/jsonl"
	"poly-gocopy/internal/polygonwatch"
	"poly-gocopy/internal/state"
)

type args struct {
	leaders          []common.Address
	leadersCanonical []common.Address
	leaderSet        map[common.Address]struct{}
	leaderTopics     []common.Hash
	polygonWs        string
	startBlock       uint64
	confirmations    uint64

	copyBps       int64
	exactAsLeader bool
	backfill      bool

	clobHost       string
	privateKeyHex  string
	funder         common.Address
	signatureType  int
	apiKey         string
	apiSecret      string
	apiPassphrase  string
	apiNonce       uint64
	useServerTime  bool
	orderType      clob.OrderType
	enableTrading  bool
	checkpointFile string
	outFile        string

	txHash     string
	txLogIndex uint64
}

const defaultTradesOutFile = "./out/trades.jsonl"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}
	log.Printf("[warn] copytrade is deprecated and no longer supported; use arbitrage strategies instead")

	parsed, err := parseArgs()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	runStartedAt := time.Now()
	tradeLog := jsonl.New(parsed.outFile)
	if tradeLog != nil {
		log.Printf("Trade log: %s (JSONL)", parsed.outFile)
		leaderForEvent := ""
		if len(parsed.leaders) == 1 {
			leaderForEvent = parsed.leaders[0].Hex()
		}
		leadersForEvent := leaderHexes(parsed.leadersCanonical)
		defer func() {
			if err := tradeLog.Close(); err != nil {
				log.Printf("[warn] trade log close: %v", err)
			}
		}()
		defer func() {
			logCopyEvent(tradeLog, copyLogEvent{
				TsMs:          time.Now().UnixMilli(),
				Event:         "shutdown",
				Mode:          copyMode(parsed.enableTrading),
				Leader:        leaderForEvent,
				Leaders:       leadersForEvent,
				CopyBps:       parsed.copyBps,
				ExactAsLeader: parsed.exactAsLeader,
				Backfill:      parsed.backfill,
				Confirmations: parsed.confirmations,
				OrderType:     string(parsed.orderType),
				EnableTrading: parsed.enableTrading,
				Ok:            true,
				UptimeMs:      time.Since(runStartedAt).Milliseconds(),
			})
		}()
		logCopyEvent(tradeLog, copyLogEvent{
			TsMs:          time.Now().UnixMilli(),
			Event:         "start",
			Mode:          copyMode(parsed.enableTrading),
			Leader:        leaderForEvent,
			Leaders:       leadersForEvent,
			CopyBps:       parsed.copyBps,
			ExactAsLeader: parsed.exactAsLeader,
			Backfill:      parsed.backfill,
			Confirmations: parsed.confirmations,
			OrderType:     string(parsed.orderType),
			EnableTrading: parsed.enableTrading,
			UptimeMs:      0,
		})
	}

	log.Printf("Copytrade (taker-only, confirmed fills) → Polymarket CLOB")
	if len(parsed.leaders) == 1 {
		log.Printf("Leader: %s", parsed.leaders[0].Hex())
	} else {
		log.Printf("Leaders: %s", ethutil.JoinHex(parsed.leaders))
	}
	if parsed.enableTrading && len(parsed.leaders) > 1 {
		log.Printf("[warn] enable-trading with multiple leaders: orders may follow any configured leader")
	}
	log.Printf("Copy bps: %d (%.2f%%)", parsed.copyBps, float64(parsed.copyBps)/100)
	log.Printf("Exact as leader: %v", parsed.exactAsLeader)
	log.Printf("Backfill: %v", parsed.backfill)
	log.Printf("Confirmations: %d", parsed.confirmations)
	log.Printf("Dry-run: %v", !parsed.enableTrading)

	pk, err := parsePrivateKey(parsed.privateKeyHex)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	clobClient, err := clob.NewClient(parsed.clobHost, 137, pk, parsed.funder, parsed.signatureType)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		log.Printf("Shutting down...")
		cancel()
	}()

	if parsed.apiKey != "" && parsed.apiSecret != "" && parsed.apiPassphrase != "" {
		clobClient.SetApiCreds(clob.ApiKeyCreds{Key: parsed.apiKey, Secret: parsed.apiSecret, Passphrase: parsed.apiPassphrase})
	} else if parsed.enableTrading {
		creds, err := clobClient.CreateOrDeriveApiKey(ctx, parsed.apiNonce, parsed.useServerTime)
		if err != nil {
			log.Fatalf("[fatal] failed to create/derive api key: %v", err)
		}
		clobClient.SetApiCreds(creds)
		log.Printf("CLOB API creds ready (key=%s…)", safePrefix(creds.Key, 8))
	}

	if parsed.txHash != "" {
		polygon, _, err := dialPolygonWithBackoff(ctx, parsed.polygonWs, time.Second, 30*time.Second)
		if err != nil {
			// Context cancellation (SIGINT/SIGTERM) is the only expected way to get here.
			return
		}
		defer polygon.Close()

		if err := inspectTx(ctx, polygon, parsed, clobClient, tradeLog, runStartedAt); err != nil {
			log.Fatalf("[fatal] inspect tx failed: %v", err)
		}
		return
	}

	ckpt, hasCkpt, err := state.LoadCheckpoint(parsed.checkpointFile)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}
	if hasCkpt {
		log.Printf("Loaded checkpoint %s (block=%d idx=%d)", parsed.checkpointFile, ckpt.LastProcessedBlock, ckpt.LastProcessedLogIndex)
	}

	polygon, headNum, err := dialPolygonWithBackoff(ctx, parsed.polygonWs, time.Second, 30*time.Second)
	if err != nil {
		// Context cancellation (SIGINT/SIGTERM) is the only expected way to get here.
		return
	}

	// Head.
	if hasCkpt && ckpt.LastProcessedBlock > headNum {
		log.Printf("[warn] checkpoint block=%d is ahead of current head=%d; ignoring checkpoint cursor", ckpt.LastProcessedBlock, headNum)
		hasCkpt = false
		ckpt = state.Checkpoint{}
	}

	// Defaults: start at head. Backfill only when explicitly enabled.
	startBlock := headNum
	useCheckpointCursor := false
	switch {
	case !parsed.backfill:
		if parsed.startBlock != 0 {
			log.Printf("[warn] --start-block ignored unless --backfill is enabled")
		}
		if hasCkpt {
			log.Printf("[info] checkpoint ignored (backfill disabled)")
		}
	case parsed.startBlock != 0:
		startBlock = parsed.startBlock
	case hasCkpt && checkpointLeadersMatch(ckpt, parsed.leadersCanonical) && checkpointExchangeCompatible(ckpt.ExchangeAddress):
		startBlock = ckpt.LastProcessedBlock
		useCheckpointCursor = true
	default:
		startBlock = headNum
	}
	log.Printf("Start block: %d (current head=%d)", startBlock, headNum)

	pending := newPendingQueue()
	cursor := ckptCursor{block: startBlock, index: ^uint(0)}
	if parsed.backfill {
		cursor = cursorBeforeBlock(startBlock)
	}
	if useCheckpointCursor {
		cursor = ckptCursor{block: ckpt.LastProcessedBlock, index: ckpt.LastProcessedLogIndex}
	} else {
		leadersForCheckpoint := leaderHexes(parsed.leadersCanonical)
		ckpt = state.Checkpoint{
			ChainID:               137,
			LeaderAddress:         leadersForCheckpoint[0],
			LeaderAddresses:       leadersForCheckpoint,
			ExchangeAddress:       exchangeAddresses[0].Hex(),
			LastProcessedBlock:    cursor.block,
			LastProcessedLogIndex: cursor.index,
		}
	}

	// Backfill.
	if parsed.backfill && startBlock <= headNum {
		log.Printf("Backfilling logs [%d..%d]...", startBlock, headNum)
		if err := backfill(ctx, polygon, startBlock, headNum, pending, cursor, parsed.leaderSet, parsed.leaderTopics); err != nil {
			log.Fatalf("[fatal] backfill failed: %v", err)
		}
	}

	readyBefore := uint64(0)
	if headNum > parsed.confirmations {
		readyBefore = headNum - parsed.confirmations
	}
	if err := processReady(ctx, polygon, pending, readyBefore, cursor, parsed, clobClient, &ckpt, tradeLog, runStartedAt); err != nil {
		log.Printf("[warn] process error: %v", err)
	}
	cursor = ckptCursor{block: ckpt.LastProcessedBlock, index: ckpt.LastProcessedLogIndex}

	latestHead := headNum
	log.Printf("Listening…")

	for {
		sessionCtx, sessionCancel := context.WithCancel(ctx)

		logsCh := make(chan types.Log, 2048)
		logSub, err := polygon.SubscribeFilterLogs(sessionCtx, orderFilledQuery(parsed.leaderTopics), logsCh)
		if err != nil {
			sessionCancel()
			polygon.Close()
			polygon, headNum, err = reconnectPolygon(ctx, parsed.polygonWs, cursor, startBlock, pending, parsed.leaderSet, parsed.leaderTopics, parsed.backfill)
			if err != nil {
				return
			}
			latestHead = headNum
			continue
		}

		headsCh := make(chan *types.Header, 16)
		headSub, err := polygon.SubscribeNewHead(sessionCtx, headsCh)
		if err != nil {
			logSub.Unsubscribe()
			sessionCancel()
			polygon.Close()
			polygon, headNum, err = reconnectPolygon(ctx, parsed.polygonWs, cursor, startBlock, pending, parsed.leaderSet, parsed.leaderTopics, parsed.backfill)
			if err != nil {
				return
			}
			latestHead = headNum
			continue
		}

		needsReconnect := false
		for !needsReconnect {
			select {
			case <-ctx.Done():
				headSub.Unsubscribe()
				logSub.Unsubscribe()
				sessionCancel()
				polygon.Close()
				return

			case err := <-logSub.Err():
				if err != nil {
					log.Printf("[warn] log subscription error: %v", err)
				} else {
					log.Printf("[warn] log subscription ended")
				}
				needsReconnect = true

			case err := <-headSub.Err():
				if err != nil {
					log.Printf("[warn] head subscription error: %v", err)
				} else {
					log.Printf("[warn] head subscription ended")
				}
				needsReconnect = true

			case hdr := <-headsCh:
				if hdr == nil {
					continue
				}
				latestHead = hdr.Number.Uint64()
				readyBefore := uint64(0)
				if latestHead > parsed.confirmations {
					readyBefore = latestHead - parsed.confirmations
				}
				if err := processReady(ctx, polygon, pending, readyBefore, cursor, parsed, clobClient, &ckpt, tradeLog, runStartedAt); err != nil {
					log.Printf("[warn] process error: %v", err)
				}
				cursor = ckptCursor{block: ckpt.LastProcessedBlock, index: ckpt.LastProcessedLogIndex}

			case vLog := <-logsCh:
				if len(vLog.Topics) < 1 || vLog.Topics[0] != orderFilledTopic {
					continue
				}
				if len(vLog.Topics) < 4 {
					continue
				}
				// Fast taker filter before heavy decode.
				taker := common.BytesToAddress(vLog.Topics[3].Bytes())
				if _, ok := parsed.leaderSet[taker]; !ok {
					continue
				}

				rxMs := time.Now().UnixMilli()
				fill, err := polygonwatch.DecodeOrderFilledLog(vLog)
				if err != nil {
					log.Printf("[warn] decode OrderFilled failed: %v", err)
					continue
				}
				fill.ReceivedAtMs = rxMs
				pending.Upsert(fill)
			}
		}

		headSub.Unsubscribe()
		logSub.Unsubscribe()
		sessionCancel()
		polygon.Close()

		polygon, headNum, err = reconnectPolygon(ctx, parsed.polygonWs, cursor, startBlock, pending, parsed.leaderSet, parsed.leaderTopics, parsed.backfill)
		if err != nil {
			return
		}
		latestHead = headNum

		readyBefore := uint64(0)
		if latestHead > parsed.confirmations {
			readyBefore = latestHead - parsed.confirmations
		}
		if err := processReady(ctx, polygon, pending, readyBefore, cursor, parsed, clobClient, &ckpt, tradeLog, runStartedAt); err != nil {
			log.Printf("[warn] process error: %v", err)
		}
		cursor = ckptCursor{block: ckpt.LastProcessedBlock, index: ckpt.LastProcessedLogIndex}
	}
}

func inspectTx(ctx context.Context, client *ethclient.Client, parsed args, clobClient *clob.Client, tradeLog *jsonl.Writer, runStartedAt time.Time) error {
	if client == nil {
		return fmt.Errorf("polygon client required")
	}
	if clobClient == nil {
		return fmt.Errorf("clob client required")
	}
	if len(parsed.leaders) != 1 {
		return fmt.Errorf("tx inspection requires exactly 1 leader (got %d)", len(parsed.leaders))
	}
	if strings.TrimSpace(parsed.txHash) == "" {
		return fmt.Errorf("tx hash required")
	}
	if parsed.enableTrading && !clobClient.HasApiCreds() {
		return fmt.Errorf("enable-trading requires CLOB API creds (set CLOB_API_KEY/CLOB_SECRET/CLOB_PASSPHRASE or allow the tool to derive them)")
	}

	leader := parsed.leaders[0]
	txHash := common.HexToHash(parsed.txHash)
	receipt, err := client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return fmt.Errorf("fetch receipt: %w", err)
	}
	if receipt == nil {
		return fmt.Errorf("receipt missing")
	}

	blockNum := uint64(0)
	if receipt.BlockNumber != nil {
		blockNum = receipt.BlockNumber.Uint64()
	}

	log.Printf("[inspect] tx=%s block=%d status=%d logs=%d", txHash.Hex(), blockNum, receipt.Status, len(receipt.Logs))

	seenOrderFilled := 0
	leaderFills := make([]*polygonwatch.FillEvent, 0, 4)

	for _, lg := range receipt.Logs {
		if lg == nil {
			continue
		}
		if !checkpointExchangeCompatible(lg.Address.Hex()) {
			continue
		}
		if len(lg.Topics) < 1 || lg.Topics[0] != orderFilledTopic {
			continue
		}
		seenOrderFilled++

		fill, err := polygonwatch.DecodeOrderFilledLog(*lg)
		if err != nil {
			log.Printf("[warn] inspect decode OrderFilled failed: %v", err)
			continue
		}
		if parsed.txLogIndex != ^uint64(0) && uint64(fill.LogIndex) != parsed.txLogIndex {
			continue
		}
		if fill.Taker != leader {
			continue
		}
		leaderFills = append(leaderFills, fill)
	}

	log.Printf("[inspect] found: orderfilled=%d leader-matches=%d", seenOrderFilled, len(leaderFills))
	if len(leaderFills) == 0 {
		log.Printf("[inspect] note: no OrderFilled logs found where taker==leader; try a different tx or remove/adjust --leader")
		return nil
	}

	receiptTrade, receiptErr := copytrade.InferLeaderTradeFromReceipt(receipt, leader, exchangeAddresses[:], collateralUSDC)
	if receiptErr == nil && receiptTrade != nil {
		repFill := leaderFills[0]
		if repFill == nil {
			return fmt.Errorf("unexpected nil fill")
		}

		var amount *big.Int
		if parsed.exactAsLeader {
			amount = new(big.Int).Set(receiptTrade.AmountUnits)
		} else {
			amount = scaleByBps(receiptTrade.AmountUnits, parsed.copyBps)
		}
		if amount.Sign() <= 0 {
			log.Printf("[warn] inspect amount scaled to 0, skip tx=%s idx=%d", repFill.TxHash.Hex(), repFill.LogIndex)
			return nil
		}

		salt := deterministicSalt(repFill, clobClient.SignerAddress())
		saltGen := func() int64 { return salt }

		var (
			orderRes *clob.OrderResult
			err      error
		)
		if parsed.exactAsLeader {
			if receiptTrade.MakerAmountUnits == nil || receiptTrade.MakerAmountUnits.Sign() <= 0 || receiptTrade.TakerAmountUnits == nil || receiptTrade.TakerAmountUnits.Sign() <= 0 {
				return fmt.Errorf("missing receipt amounts for exact-as-leader")
			}
			makerAmount := new(big.Int).Set(receiptTrade.MakerAmountUnits)
			takerAmount := new(big.Int).Set(receiptTrade.TakerAmountUnits)
			orderRes, err = clobClient.CreateSignedExactOrder(ctx, receiptTrade.TokenID, receiptTrade.Side, makerAmount, takerAmount, saltGen)
		} else {
			orderRes, err = clobClient.CreateSignedMarketOrder(ctx, receiptTrade.TokenID, receiptTrade.Side, amount, parsed.orderType, parsed.useServerTime, saltGen)
		}
		if err != nil {
			return fmt.Errorf("inspect create order failed (token=%s side=%s amount=%s): %w", receiptTrade.TokenID, receiptTrade.Side, amount.String(), err)
		}

		body, _ := clobClient.BuildPostOrderBody(orderRes.SignedOrder, parsed.orderType, false)
		mode := "scaled-market"
		if parsed.exactAsLeader {
			mode = "exact-as-leader"
		}
		log.Printf(
			"[inspect] mode=%s tx=%s idx=%d leader=%s → receipt token=%s side=%s amount=%s copyBps=%d clobMaker=%s clobSigner=%s clobSigType=%s clobMakerAmt=%s clobTakerAmt=%s price=%s tick=%s payload=%s",
			mode,
			repFill.TxHash.Hex(),
			repFill.LogIndex,
			leader.Hex(),
			receiptTrade.TokenID,
			receiptTrade.Side,
			amount.String(),
			parsed.copyBps,
			orderRes.SignedOrder.Maker.Hex(),
			orderRes.SignedOrder.Signer.Hex(),
			bigString(orderRes.SignedOrder.SignatureType),
			bigString(orderRes.SignedOrder.MakerAmount),
			bigString(orderRes.SignedOrder.TakerAmount),
			orderRes.Price,
			orderRes.TickSize,
			strings.TrimSpace(string(body)),
		)

		if parsed.enableTrading {
			resp, _, err := clobClient.PostSignedOrder(ctx, orderRes.SignedOrder, parsed.orderType, false, parsed.useServerTime)
			if err != nil {
				logCopyEvent(tradeLog, copyLogEvent{
					TsMs:          time.Now().UnixMilli(),
					Event:         "inspect_trade",
					Mode:          "live",
					Leader:        leader.Hex(),
					TxHash:        repFill.TxHash.Hex(),
					LogIndex:      uint64(repFill.LogIndex),
					TokenID:       receiptTrade.TokenID,
					Side:          string(receiptTrade.Side),
					AmountUnits:   amount.String(),
					CopyBps:       parsed.copyBps,
					ExactAsLeader: parsed.exactAsLeader,
					Price:         orderRes.Price,
					TickSize:      orderRes.TickSize,
					OrderType:     string(parsed.orderType),
					EnableTrading: true,
					Ok:            false,
					Err:           err.Error(),
					UptimeMs:      time.Since(runStartedAt).Milliseconds(),
				})
				return fmt.Errorf("inspect post order failed: %w", err)
			}
			log.Printf("[trade] tx=%s idx=%d → resp=%v", repFill.TxHash.Hex(), repFill.LogIndex, resp)
			logCopyEvent(tradeLog, copyLogEvent{
				TsMs:          time.Now().UnixMilli(),
				Event:         "inspect_trade",
				Mode:          "live",
				Leader:        leader.Hex(),
				TxHash:        repFill.TxHash.Hex(),
				LogIndex:      uint64(repFill.LogIndex),
				TokenID:       receiptTrade.TokenID,
				Side:          string(receiptTrade.Side),
				AmountUnits:   amount.String(),
				CopyBps:       parsed.copyBps,
				ExactAsLeader: parsed.exactAsLeader,
				Price:         orderRes.Price,
				TickSize:      orderRes.TickSize,
				OrderType:     string(parsed.orderType),
				EnableTrading: true,
				Ok:            true,
				Resp:          resp,
				UptimeMs:      time.Since(runStartedAt).Milliseconds(),
			})
		} else {
			logCopyEvent(tradeLog, copyLogEvent{
				TsMs:          time.Now().UnixMilli(),
				Event:         "inspect_dry_run",
				Mode:          "dry",
				Leader:        leader.Hex(),
				TxHash:        repFill.TxHash.Hex(),
				LogIndex:      uint64(repFill.LogIndex),
				TokenID:       receiptTrade.TokenID,
				Side:          string(receiptTrade.Side),
				AmountUnits:   amount.String(),
				CopyBps:       parsed.copyBps,
				ExactAsLeader: parsed.exactAsLeader,
				Price:         orderRes.Price,
				TickSize:      orderRes.TickSize,
				OrderType:     string(parsed.orderType),
				EnableTrading: false,
				Ok:            true,
				UptimeMs:      time.Since(runStartedAt).Milliseconds(),
			})
		}
		return nil
	}

	if receiptErr != nil {
		log.Printf("[warn] inspect receipt infer failed (falling back to OrderFilled): %v", receiptErr)
	}

	if parsed.enableTrading && len(leaderFills) != 1 {
		if parsed.txLogIndex == ^uint64(0) {
			return fmt.Errorf("inspect trading requires exactly 1 leader-matching OrderFilled; found %d (pass --tx-log-index to select one)", len(leaderFills))
		}
		return fmt.Errorf("inspect trading expected exactly 1 fill after --tx-log-index filter, found %d", len(leaderFills))
	}

	// Fallback: per-fill inference based on OrderFilled.
	for _, fill := range leaderFills {
		if fill == nil {
			continue
		}
		trade, err := copytrade.ComputeLeaderTakerTrade(fill, leader, collateralUSDC)
		if err != nil {
			log.Printf("[warn] inspect skip fill tx=%s idx=%d: %v", fill.TxHash.Hex(), fill.LogIndex, err)
			continue
		}
		if trade == nil {
			continue
		}

		var amount *big.Int
		if parsed.exactAsLeader {
			amount = new(big.Int).Set(trade.AmountUnits)
		} else {
			amount = scaleByBps(trade.AmountUnits, parsed.copyBps)
		}
		if amount.Sign() <= 0 {
			log.Printf("[warn] inspect amount scaled to 0, skip tx=%s idx=%d", fill.TxHash.Hex(), fill.LogIndex)
			continue
		}

		salt := deterministicSalt(fill, clobClient.SignerAddress())
		saltGen := func() int64 { return salt }

		var orderRes *clob.OrderResult
		if parsed.exactAsLeader {
			if fill.MakerAmountFilled == nil || fill.MakerAmountFilled.Sign() <= 0 || fill.TakerAmountFilled == nil || fill.TakerAmountFilled.Sign() <= 0 {
				log.Printf("[warn] inspect skip exact-as-leader (missing filled amounts) tx=%s idx=%d", fill.TxHash.Hex(), fill.LogIndex)
				continue
			}
			var makerAmount, takerAmount *big.Int
			switch trade.Side {
			case clob.SideBuy:
				makerAmount = new(big.Int).Set(fill.TakerAmountFilled) // collateral
				takerAmount = new(big.Int).Set(fill.MakerAmountFilled) // shares
			case clob.SideSell:
				makerAmount = new(big.Int).Set(fill.TakerAmountFilled) // shares
				takerAmount = new(big.Int).Set(fill.MakerAmountFilled) // collateral
			default:
				log.Printf("[warn] inspect unsupported side %q tx=%s idx=%d", trade.Side, fill.TxHash.Hex(), fill.LogIndex)
				continue
			}
			orderRes, err = clobClient.CreateSignedExactOrder(ctx, trade.TokenID, trade.Side, makerAmount, takerAmount, saltGen)
		} else {
			orderRes, err = clobClient.CreateSignedMarketOrder(ctx, trade.TokenID, trade.Side, amount, parsed.orderType, parsed.useServerTime, saltGen)
		}
		if err != nil {
			log.Printf("[warn] inspect create order failed (token=%s side=%s amount=%s): %v", trade.TokenID, trade.Side, amount.String(), err)
			continue
		}

		body, _ := clobClient.BuildPostOrderBody(orderRes.SignedOrder, parsed.orderType, false)
		mode := "scaled-market"
		if parsed.exactAsLeader {
			mode = "exact-as-leader"
		}

		log.Printf(
			"[inspect] mode=%s fill tx=%s idx=%d leader=%s evMaker=%s evTaker=%s evMakerAssetId=%s evTakerAssetId=%s evMakerFilled=%s evTakerFilled=%s → copy token=%s side=%s amount=%s copyBps=%d clobMaker=%s clobSigner=%s clobSigType=%s clobMakerAmt=%s clobTakerAmt=%s price=%s tick=%s payload=%s",
			mode,
			fill.TxHash.Hex(),
			fill.LogIndex,
			leader.Hex(),
			fill.Maker.Hex(),
			fill.Taker.Hex(),
			bigString(fill.MakerAssetId),
			bigString(fill.TakerAssetId),
			bigString(fill.MakerAmountFilled),
			bigString(fill.TakerAmountFilled),
			trade.TokenID,
			trade.Side,
			amount.String(),
			parsed.copyBps,
			orderRes.SignedOrder.Maker.Hex(),
			orderRes.SignedOrder.Signer.Hex(),
			bigString(orderRes.SignedOrder.SignatureType),
			bigString(orderRes.SignedOrder.MakerAmount),
			bigString(orderRes.SignedOrder.TakerAmount),
			orderRes.Price,
			orderRes.TickSize,
			strings.TrimSpace(string(body)),
		)

		if parsed.enableTrading {
			resp, _, err := clobClient.PostSignedOrder(ctx, orderRes.SignedOrder, parsed.orderType, false, parsed.useServerTime)
			if err != nil {
				logCopyEvent(tradeLog, copyLogEvent{
					TsMs:          time.Now().UnixMilli(),
					Event:         "inspect_trade",
					Mode:          "live",
					Leader:        leader.Hex(),
					TxHash:        fill.TxHash.Hex(),
					LogIndex:      uint64(fill.LogIndex),
					TokenID:       trade.TokenID,
					Side:          string(trade.Side),
					AmountUnits:   amount.String(),
					CopyBps:       parsed.copyBps,
					ExactAsLeader: parsed.exactAsLeader,
					Price:         orderRes.Price,
					TickSize:      orderRes.TickSize,
					OrderType:     string(parsed.orderType),
					EnableTrading: true,
					Ok:            false,
					Err:           err.Error(),
					UptimeMs:      time.Since(runStartedAt).Milliseconds(),
				})
				return fmt.Errorf("inspect post order failed: %w", err)
			}
			log.Printf("[trade] fill tx=%s idx=%d → resp=%v", fill.TxHash.Hex(), fill.LogIndex, resp)
			logCopyEvent(tradeLog, copyLogEvent{
				TsMs:          time.Now().UnixMilli(),
				Event:         "inspect_trade",
				Mode:          "live",
				Leader:        leader.Hex(),
				TxHash:        fill.TxHash.Hex(),
				LogIndex:      uint64(fill.LogIndex),
				TokenID:       trade.TokenID,
				Side:          string(trade.Side),
				AmountUnits:   amount.String(),
				CopyBps:       parsed.copyBps,
				ExactAsLeader: parsed.exactAsLeader,
				Price:         orderRes.Price,
				TickSize:      orderRes.TickSize,
				OrderType:     string(parsed.orderType),
				EnableTrading: true,
				Ok:            true,
				Resp:          resp,
				UptimeMs:      time.Since(runStartedAt).Milliseconds(),
			})
		} else {
			logCopyEvent(tradeLog, copyLogEvent{
				TsMs:          time.Now().UnixMilli(),
				Event:         "inspect_dry_run",
				Mode:          "dry",
				Leader:        leader.Hex(),
				TxHash:        fill.TxHash.Hex(),
				LogIndex:      uint64(fill.LogIndex),
				TokenID:       trade.TokenID,
				Side:          string(trade.Side),
				AmountUnits:   amount.String(),
				CopyBps:       parsed.copyBps,
				ExactAsLeader: parsed.exactAsLeader,
				Price:         orderRes.Price,
				TickSize:      orderRes.TickSize,
				OrderType:     string(parsed.orderType),
				EnableTrading: false,
				Ok:            true,
				UptimeMs:      time.Since(runStartedAt).Milliseconds(),
			})
		}
	}

	log.Printf("[inspect] done: orderfilled=%d leader-matches=%d", seenOrderFilled, len(leaderFills))

	return nil
}

func cursorBeforeBlock(block uint64) ckptCursor {
	if block == 0 {
		return ckptCursor{block: 0, index: 0}
	}
	return ckptCursor{block: block - 1, index: 0}
}

func dialPolygonWithBackoff(ctx context.Context, url string, baseDelay, maxDelay time.Duration) (*ethclient.Client, uint64, error) {
	if baseDelay <= 0 {
		baseDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	delay := baseDelay
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		client, err := ethclient.DialContext(ctx, url)
		if err == nil {
			headNum, headErr := client.BlockNumber(ctx)
			if headErr == nil {
				return client, headNum, nil
			}
			client.Close()
			err = fmt.Errorf("failed to fetch head: %w", headErr)
		}

		wait := jitterDuration(delay)
		log.Printf("[warn] failed to connect polygon ws, retrying in %s: %v", wait, err)
		if err := sleepWithContext(ctx, wait); err != nil {
			return nil, 0, err
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func reconnectPolygon(
	ctx context.Context,
	url string,
	cursor ckptCursor,
	floorBlock uint64,
	pending *pendingQueue,
	leaderSet map[common.Address]struct{},
	leaderTopics []common.Hash,
	gapFill bool,
) (*ethclient.Client, uint64, error) {
	log.Printf("[warn] reconnecting polygon ws…")

	if !gapFill {
		client, headNum, err := dialPolygonWithBackoff(ctx, url, time.Second, 30*time.Second)
		if err != nil {
			return nil, 0, err
		}
		log.Printf("[warn] reconnected (head=%d; gap-fill disabled)", headNum)
		return client, headNum, nil
	}

	if pending == nil {
		return nil, 0, fmt.Errorf("pending queue required when gap-fill enabled")
	}

	delay := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		client, headNum, err := dialPolygonWithBackoff(ctx, url, time.Second, 30*time.Second)
		if err != nil {
			return nil, 0, err
		}

		from := cursor.block
		// Special cursor value meaning "treat this entire block as already processed"
		// (used when starting in live mode with --backfill=false). In that case, the earliest
		// block that can contain missed events is the next block.
		if cursor.index == ^uint(0) {
			from++
		}
		if from < floorBlock {
			from = floorBlock
		}
		if from <= headNum {
			log.Printf("[warn] gap-filling missed logs [%d..%d]...", from, headNum)
			if err := backfill(ctx, client, from, headNum, pending, cursor, leaderSet, leaderTopics); err != nil {
				client.Close()
				wait := jitterDuration(delay)
				log.Printf("[warn] gap-fill failed, retrying in %s: %v", wait, err)
				if err := sleepWithContext(ctx, wait); err != nil {
					return nil, 0, err
				}
				delay *= 2
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
				continue
			}
		}

		log.Printf("[warn] reconnected (head=%d)", headNum)
		return client, headNum, nil
	}
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	j := d / 5 // +/-20%
	if j <= 0 {
		return d
	}
	return d - j + time.Duration(rand.Int64N(int64(j*2)+1))
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
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

// --- chain + event constants ---

var (
	// Polymarket has deployed multiple exchange contracts on Polygon that emit the same OrderFilled event.
	// We subscribe to all known addresses to avoid missing fills.
	exchangeAddresses = [...]common.Address{
		common.HexToAddress("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
		common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
	}
	collateralUSDC   = common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
	orderFilledSig   = []byte("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
	orderFilledTopic = crypto.Keccak256Hash(orderFilledSig)
)

func orderFilledQuery(takerTopics []common.Hash) ethereum.FilterQuery {
	topics := [][]common.Hash{{orderFilledTopic}}
	if len(takerTopics) > 0 {
		// topics:
		// 0: event sig
		// 1: orderHash (indexed)
		// 2: maker (indexed)
		// 3: taker (indexed)
		topics = [][]common.Hash{{orderFilledTopic}, nil, nil, takerTopics}
	}
	return ethereum.FilterQuery{
		Addresses: exchangeAddresses[:],
		Topics:    topics,
	}
}

func checkpointExchangeCompatible(exchange string) bool {
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return true
	}
	if !common.IsHexAddress(exchange) {
		return false
	}
	addr := common.HexToAddress(exchange)
	for _, ex := range exchangeAddresses {
		if ex == addr {
			return true
		}
	}
	return false
}

func leaderHexes(addrs []common.Address) []string {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.Hex())
	}
	return out
}

func checkpointLeadersMatch(ckpt state.Checkpoint, leadersCanonical []common.Address) bool {
	want := leaderHexes(leadersCanonical)
	if len(want) == 0 {
		return false
	}

	// Preferred multi-leader format.
	if len(ckpt.LeaderAddresses) > 0 {
		got := append([]string(nil), ckpt.LeaderAddresses...)
		sort.Slice(got, func(i, j int) bool {
			return strings.ToLower(got[i]) < strings.ToLower(got[j])
		})
		sort.Slice(want, func(i, j int) bool {
			return strings.ToLower(want[i]) < strings.ToLower(want[j])
		})
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if !strings.EqualFold(got[i], want[i]) {
				return false
			}
		}
		return true
	}

	// Legacy single-leader format.
	if len(want) == 1 {
		return strings.EqualFold(strings.TrimSpace(ckpt.LeaderAddress), want[0])
	}
	return false
}

// --- args ---

func parseArgs() (args, error) {
	var leaderFlag string
	var polygonWsFlag string
	var startBlockFlag uint64
	var confsFlag uint64
	var copyBpsFlag int64
	var copyPctFlag float64
	var exactAsLeaderFlag bool
	var backfillFlag bool

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
	var enableTradingFlag bool

	enableTradingDefault := false
	if env := strings.TrimSpace(os.Getenv("ENABLE_TRADING")); env != "" {
		v, err := strconv.ParseBool(env)
		if err != nil {
			return args{}, fmt.Errorf("invalid ENABLE_TRADING %q: %w", env, err)
		}
		enableTradingDefault = v
	}

	var checkpointFlag string
	var outFlag string
	var txHashFlag string
	txLogIndexFlag := ^uint64(0)

	flag.StringVar(&leaderFlag, "leader", "", "Leader taker address(es) 0x... (comma/space-separated; or LEADER_PROXIES/LEADER_PROXY)")
	flag.StringVar(&leaderFlag, "leaders", "", "Leader taker address(es) 0x... (comma/space-separated) (alias)")
	flag.StringVar(&polygonWsFlag, "polygon-ws", "", "Polygon WebSocket RPC URL (wss://...)")
	flag.Uint64Var(&startBlockFlag, "start-block", 0, "Backfill start block (requires --backfill; 0 = checkpoint/head)")
	flag.Uint64Var(&confsFlag, "confirmations", 2, "Blocks to wait before acting")

	flag.Int64Var(&copyBpsFlag, "copy-bps", 0, "Copy size in basis points (e.g. 2500=25%)")
	flag.Float64Var(&copyPctFlag, "copy-pct", 0, "Copy size in percent (e.g. 25.0)")
	flag.BoolVar(&exactAsLeaderFlag, "exact-as-leader", false, "Copy leader's fills at exact size/price (ignores copy-bps/copy-pct)")
	flag.BoolVar(&backfillFlag, "backfill", false, "Backfill past logs from --start-block/checkpoint before listening (default false)")

	flag.StringVar(&clobHostFlag, "clob-url", "", "CLOB API base URL (default https://clob.polymarket.com)")
	flag.StringVar(&privateKeyFlag, "private-key", "", "Private key hex (0x...) (or PRIVATE_KEY env)")
	flag.StringVar(&funderFlag, "funder", "", "Funder address (proxy wallet) (default: signer)")
	flag.IntVar(&signatureTypeFlag, "signature-type", signatureTypeDefault, "Signature type: 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE")

	flag.StringVar(&apiKeyFlag, "api-key", "", "CLOB API key (optional; otherwise derived if trading enabled)")
	flag.StringVar(&apiSecretFlag, "api-secret", "", "CLOB API secret (optional)")
	flag.StringVar(&apiPassphraseFlag, "api-passphrase", "", "CLOB API passphrase (optional)")
	flag.Uint64Var(&apiNonceFlag, "api-nonce", 0, "Nonce for API key derive/create")
	flag.BoolVar(&useServerTimeFlag, "use-server-time", true, "Use /time for signed requests")
	flag.StringVar(&orderTypeFlag, "order-type", "FOK", "Order type: FOK or FAK")
	flag.BoolVar(&enableTradingFlag, "enable-trading", enableTradingDefault, "Actually place trades (default is dry-run)")

	flag.StringVar(&checkpointFlag, "checkpoint-file", "./out/copytrade.checkpoint.json", "Checkpoint path")
	flag.StringVar(&outFlag, "out", "", "Optional output file path (JSONL; logs copytrade decisions + trade responses)")
	flag.StringVar(&outFlag, "outfile", "", "Optional output file path (JSONL; logs copytrade decisions + trade responses) (alias)")
	flag.StringVar(&txHashFlag, "tx-hash", "", "Inspect a specific tx hash (0x...) and exit (no subscriptions; use --enable-trading to execute)")
	flag.Uint64Var(&txLogIndexFlag, "tx-log-index", txLogIndexFlag, "Optional log index filter for --tx-hash (default: all)")

	flag.Parse()

	txHash := strings.ToLower(strings.TrimSpace(txHashFlag))
	if txHash != "" {
		txHash = strings.TrimPrefix(txHash, "0x")
		if len(txHash) != 64 {
			return args{}, fmt.Errorf("invalid --tx-hash: expected 0x + 64 hex chars")
		}
		if _, err := hex.DecodeString(txHash); err != nil {
			return args{}, fmt.Errorf("invalid --tx-hash: %w", err)
		}
		txHash = "0x" + txHash
	}

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
	if txHash != "" && len(leaders) != 1 {
		return args{}, fmt.Errorf("--tx-hash requires exactly 1 leader (got %d)", len(leaders))
	}
	leadersCanonical := ethutil.SortedAddresses(leaders)
	leaderSet := ethutil.AddressSet(leaders)
	leaderTopics := ethutil.AddressesToTopics(leaders)

	envWs := firstNonEmpty(os.Getenv("RPC_WS_URL"), os.Getenv("RPC_URL"), os.Getenv("POLYGON_WS_URL"))
	polygonWs := polygonWsFlag
	if polygonWs == "" {
		polygonWs = envWs
	}
	if polygonWs == "" {
		return args{}, fmt.Errorf("polygon ws required via --polygon-ws or RPC_WS_URL")
	}
	if txHash == "" {
		if !strings.HasPrefix(polygonWs, "wss") {
			return args{}, fmt.Errorf("polygon-ws must be wss://... (got %q)", polygonWs)
		}
	} else {
		if !strings.HasPrefix(polygonWs, "wss") && !strings.HasPrefix(polygonWs, "http") {
			return args{}, fmt.Errorf("polygon-ws must be wss://... or http(s)://... when using --tx-hash (got %q)", polygonWs)
		}
	}

	pkHex := strings.TrimSpace(privateKeyFlag)
	if pkHex == "" {
		pkHex = firstNonEmpty(os.Getenv("CLOB_PRIVATE_KEY"), os.Getenv("PRIVATE_KEY"))
	}
	if pkHex == "" {
		return args{}, fmt.Errorf("private key required (set --private-key or PRIVATE_KEY)")
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

	// Copy percent.
	var copyBps int64
	switch {
	case copyBpsFlag > 0:
		copyBps = copyBpsFlag
	case copyPctFlag > 0:
		copyBps = int64(copyPctFlag * 100)
	default:
		if env := os.Getenv("COPY_PCT"); strings.TrimSpace(env) != "" {
			if v, err := strconv.ParseFloat(strings.TrimSpace(env), 64); err == nil && v > 0 {
				copyBps = int64(v * 100)
			}
		}
		if env := os.Getenv("COPY_BPS"); strings.TrimSpace(env) != "" {
			if v, err := strconv.ParseInt(env, 10, 64); err == nil && v > 0 {
				copyBps = v
			}
		}
		if copyBps == 0 {
			copyBps = 10000
		}
	}
	if exactAsLeaderFlag {
		copyBps = 10000
	}
	if copyBps <= 0 || copyBps > 10000 {
		return args{}, fmt.Errorf("copy bps must be in (0,10000], got %d", copyBps)
	}

	var ot clob.OrderType
	switch strings.ToUpper(strings.TrimSpace(orderTypeFlag)) {
	case "FOK":
		ot = clob.OrderTypeFOK
	case "FAK":
		ot = clob.OrderTypeFAK
	default:
		return args{}, fmt.Errorf("unsupported order-type %q (use FOK or FAK)", orderTypeFlag)
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
		outFile = strings.TrimSpace(os.Getenv("COPYTRADE_OUT_FILE"))
	}
	if outFile == "" {
		outFile = strings.TrimSpace(os.Getenv("TRADES_OUT_FILE"))
	}
	if outFile == "" {
		outFile = defaultTradesOutFile
	}

	return args{
		leaders:          leaders,
		leadersCanonical: leadersCanonical,
		leaderSet:        leaderSet,
		leaderTopics:     leaderTopics,
		polygonWs:        polygonWs,
		startBlock:       startBlockFlag,
		confirmations:    confsFlag,
		copyBps:          copyBps,
		exactAsLeader:    exactAsLeaderFlag,
		backfill:         backfillFlag,
		clobHost:         host,
		privateKeyHex:    pkHex,
		funder:           funder,
		signatureType:    signatureTypeFlag,
		apiKey:           apiKey,
		apiSecret:        apiSecret,
		apiPassphrase:    apiPass,
		apiNonce:         apiNonceFlag,
		useServerTime:    useServerTimeFlag,
		orderType:        ot,
		enableTrading:    enableTradingFlag,
		checkpointFile:   checkpointFlag,
		outFile:          outFile,
		txHash:           txHash,
		txLogIndex:       txLogIndexFlag,
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

func safePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
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

// --- backfill + processing ---

type ckptCursor struct {
	block uint64
	index uint
}

func (c ckptCursor) beforeOrEqual(fill *polygonwatch.FillEvent) bool {
	if fill == nil {
		return false
	}
	if fill.BlockNumber < c.block {
		return true
	}
	if fill.BlockNumber > c.block {
		return false
	}
	return fill.LogIndex <= c.index
}

type pendingQueue struct {
	byBlock map[uint64]map[uint]*polygonwatch.FillEvent
}

func newPendingQueue() *pendingQueue {
	return &pendingQueue{byBlock: make(map[uint64]map[uint]*polygonwatch.FillEvent)}
}

func (q *pendingQueue) Upsert(fill *polygonwatch.FillEvent) {
	if fill == nil {
		return
	}
	block := fill.BlockNumber
	if q.byBlock[block] == nil {
		q.byBlock[block] = make(map[uint]*polygonwatch.FillEvent)
	}
	if fill.Removed {
		delete(q.byBlock[block], fill.LogIndex)
		if len(q.byBlock[block]) == 0 {
			delete(q.byBlock, block)
		}
		return
	}
	if prev := q.byBlock[block][fill.LogIndex]; prev != nil {
		switch {
		case fill.ReceivedAtMs == 0:
			fill.ReceivedAtMs = prev.ReceivedAtMs
		case prev.ReceivedAtMs != 0 && prev.ReceivedAtMs < fill.ReceivedAtMs:
			fill.ReceivedAtMs = prev.ReceivedAtMs
		}
	}
	q.byBlock[block][fill.LogIndex] = fill
}

func backfill(ctx context.Context, client *ethclient.Client, from, to uint64, q *pendingQueue, cursor ckptCursor, leaderSet map[common.Address]struct{}, leaderTopics []common.Hash) error {
	if from > to {
		return nil
	}

	const maxInitialChunk uint64 = 2000
	chunk := to - from + 1
	if chunk > maxInitialChunk {
		chunk = maxInitialChunk
	}

	baseQuery := orderFilledQuery(leaderTopics)
	for start := from; start <= to; {
		end := start + chunk - 1
		if end > to {
			end = to
		}

		query := baseQuery
		query.FromBlock = new(big.Int).SetUint64(start)
		query.ToBlock = new(big.Int).SetUint64(end)

		logs, err := client.FilterLogs(ctx, query)
		if err != nil {
			if limit, ok := parseEthGetLogsRangeLimit(err); ok && limit > 0 && limit < chunk {
				chunk = limit
				log.Printf("[warn] eth_getLogs range limit detected, retrying with chunk=%d blocks", chunk)
				continue
			}
			if chunk > 1 {
				chunk = chunk / 2
				if chunk < 1 {
					chunk = 1
				}
				log.Printf("[warn] backfill query failed, retrying with chunk=%d blocks: %v", chunk, err)
				continue
			}
			return err
		}

		sort.Slice(logs, func(i, j int) bool {
			if logs[i].BlockNumber != logs[j].BlockNumber {
				return logs[i].BlockNumber < logs[j].BlockNumber
			}
			return logs[i].Index < logs[j].Index
		})

		for _, vLog := range logs {
			if len(vLog.Topics) < 1 || vLog.Topics[0] != orderFilledTopic || len(vLog.Topics) < 4 {
				continue
			}
			taker := common.BytesToAddress(vLog.Topics[3].Bytes())
			if _, ok := leaderSet[taker]; !ok {
				continue
			}
			fill, err := polygonwatch.DecodeOrderFilledLog(vLog)
			if err != nil {
				log.Printf("[warn] backfill decode failed: %v", err)
				continue
			}
			if cursor.beforeOrEqual(fill) {
				continue
			}
			fill.ReceivedAtMs = time.Now().UnixMilli()
			q.Upsert(fill)
		}

		start = end + 1
	}

	return nil
}

func processReady(
	ctx context.Context,
	polygon *ethclient.Client,
	q *pendingQueue,
	readyBefore uint64,
	cursor ckptCursor,
	parsed args,
	clobClient *clob.Client,
	ckpt *state.Checkpoint,
	tradeLog *jsonl.Writer,
	runStartedAt time.Time,
) error {
	if ckpt == nil {
		return fmt.Errorf("checkpoint pointer required")
	}
	if polygon == nil {
		return fmt.Errorf("polygon client required")
	}

	blocks := make([]uint64, 0, len(q.byBlock))
	for bn := range q.byBlock {
		if bn <= readyBefore {
			blocks = append(blocks, bn)
		}
	}
	if len(blocks) == 0 {
		return nil
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	receiptCache := make(map[common.Hash]*types.Receipt)
	blockTimeCache := make(map[uint64]int64, len(blocks))

	for _, bn := range blocks {
		blockMs := int64(-1)
		if cached, ok := blockTimeCache[bn]; ok {
			blockMs = cached
		} else {
			headerCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
			hdr, err := polygon.HeaderByNumber(headerCtx, new(big.Int).SetUint64(bn))
			cancel()
			if err != nil || hdr == nil {
				if err != nil {
					log.Printf("[warn] header fetch failed block=%d: %v", bn, err)
				} else {
					log.Printf("[warn] header missing block=%d", bn)
				}
			} else {
				blockMs = int64(hdr.Time) * 1000
			}
			blockTimeCache[bn] = blockMs
		}

		evsMap := q.byBlock[bn]
		indexes := make([]uint, 0, len(evsMap))
		for idx := range evsMap {
			indexes = append(indexes, idx)
		}
		sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

		type txRun struct {
			hash     common.Hash
			fills    []*polygonwatch.FillEvent
			first    *polygonwatch.FillEvent
			last     *polygonwatch.FillEvent
			runStart uint
			runEnd   uint
		}

		runs := make([]txRun, 0, len(indexes))
		for _, idx := range indexes {
			fill := evsMap[idx]
			if fill == nil || cursor.beforeOrEqual(fill) {
				continue
			}
			if len(runs) == 0 || runs[len(runs)-1].hash != fill.TxHash {
				runs = append(runs, txRun{
					hash:     fill.TxHash,
					fills:    []*polygonwatch.FillEvent{fill},
					first:    fill,
					last:     fill,
					runStart: fill.LogIndex,
					runEnd:   fill.LogIndex,
				})
				continue
			}
			runs[len(runs)-1].fills = append(runs[len(runs)-1].fills, fill)
			runs[len(runs)-1].last = fill
			runs[len(runs)-1].runEnd = fill.LogIndex
		}

		for _, run := range runs {
			repFill := run.first
			lastFill := run.last
			if repFill == nil || lastFill == nil {
				continue
			}

			receipt, ok := receiptCache[run.hash]
			if !ok {
				r, err := polygon.TransactionReceipt(ctx, run.hash)
				if err != nil {
					log.Printf("[warn] fetch receipt failed tx=%s: %v", run.hash.Hex(), err)
					advanceCheckpoint(parsed, ckpt, lastFill)
					continue
				}
				if r == nil {
					log.Printf("[warn] receipt missing tx=%s", run.hash.Hex())
					advanceCheckpoint(parsed, ckpt, lastFill)
					continue
				}
				receipt = r
				receiptCache[run.hash] = r
			}

			leader := repFill.Taker
			if _, ok := parsed.leaderSet[leader]; !ok {
				log.Printf("[warn] unexpected non-leader taker in queue tx=%s taker=%s", run.hash.Hex(), leader.Hex())
				advanceCheckpoint(parsed, ckpt, lastFill)
				continue
			}
			multiLeaderTx := false
			for _, f := range run.fills {
				if f == nil {
					continue
				}
				if f.Taker != leader {
					multiLeaderTx = true
					break
				}
			}

			if !multiLeaderTx {
				receiptTrade, receiptErr := copytrade.InferLeaderTradeFromReceipt(receipt, leader, exchangeAddresses[:], collateralUSDC)
				if receiptErr == nil && receiptTrade != nil {
					rxMs := repFill.ReceivedAtMs
					for _, f := range run.fills {
						if f == nil || f.ReceivedAtMs <= 0 {
							continue
						}
						if rxMs == 0 || f.ReceivedAtMs < rxMs {
							rxMs = f.ReceivedAtMs
						}
					}
					rxLagMs := int64(-1)
					if blockMs >= 0 && rxMs > 0 {
						rxLagMs = rxMs - blockMs
					}

					var amount *big.Int
					if parsed.exactAsLeader {
						amount = new(big.Int).Set(receiptTrade.AmountUnits)
					} else {
						amount = scaleByBps(receiptTrade.AmountUnits, parsed.copyBps)
					}
					if amount.Sign() <= 0 {
						log.Printf("[warn] amount scaled to 0, skip tx=%s idx=%d", repFill.TxHash.Hex(), repFill.LogIndex)
						advanceCheckpoint(parsed, ckpt, lastFill)
						continue
					}

					salt := deterministicSalt(repFill, clobClient.SignerAddress())
					saltGen := func() int64 { return salt }

					var (
						orderRes *clob.OrderResult
						err      error
					)
					if parsed.exactAsLeader {
						if receiptTrade.MakerAmountUnits == nil || receiptTrade.MakerAmountUnits.Sign() <= 0 || receiptTrade.TakerAmountUnits == nil || receiptTrade.TakerAmountUnits.Sign() <= 0 {
							log.Printf("[warn] skip exact-as-leader (missing receipt amounts) tx=%s idx=%d", repFill.TxHash.Hex(), repFill.LogIndex)
							advanceCheckpoint(parsed, ckpt, lastFill)
							continue
						}
						makerAmount := new(big.Int).Set(receiptTrade.MakerAmountUnits)
						takerAmount := new(big.Int).Set(receiptTrade.TakerAmountUnits)
						orderRes, err = clobClient.CreateSignedExactOrder(ctx, receiptTrade.TokenID, receiptTrade.Side, makerAmount, takerAmount, saltGen)
					} else {
						orderRes, err = clobClient.CreateSignedMarketOrder(ctx, receiptTrade.TokenID, receiptTrade.Side, amount, parsed.orderType, parsed.useServerTime, saltGen)
					}
					if err != nil {
						log.Printf("[warn] create order failed (token=%s side=%s amount=%s): %v", receiptTrade.TokenID, receiptTrade.Side, amount.String(), err)
						advanceCheckpoint(parsed, ckpt, lastFill)
						continue
					}

					body, _ := clobClient.BuildPostOrderBody(orderRes.SignedOrder, parsed.orderType, false)
					if !parsed.enableTrading {
						mode := "scaled-market"
						if parsed.exactAsLeader {
							mode = "exact-as-leader"
						}
						log.Printf(
							"[dry-run] mode=%s tx=%s idx=%d leader=%s → receipt token=%s side=%s amount=%s copyBps=%d clobMaker=%s clobSigner=%s clobSigType=%s clobMakerAmt=%s clobTakerAmt=%s price=%s tick=%s blockMs=%d rxMs=%d rxLagMs=%d payload=%s",
							mode,
							repFill.TxHash.Hex(),
							repFill.LogIndex,
							leader.Hex(),
							receiptTrade.TokenID,
							receiptTrade.Side,
							amount.String(),
							parsed.copyBps,
							orderRes.SignedOrder.Maker.Hex(),
							orderRes.SignedOrder.Signer.Hex(),
							bigString(orderRes.SignedOrder.SignatureType),
							bigString(orderRes.SignedOrder.MakerAmount),
							bigString(orderRes.SignedOrder.TakerAmount),
							orderRes.Price,
							orderRes.TickSize,
							blockMs,
							rxMs,
							rxLagMs,
							strings.TrimSpace(string(body)),
						)
						logCopyEvent(tradeLog, copyLogEvent{
							TsMs:          time.Now().UnixMilli(),
							Event:         "dry_run",
							Mode:          "dry",
							Leader:        leader.Hex(),
							TxHash:        repFill.TxHash.Hex(),
							LogIndex:      uint64(repFill.LogIndex),
							TokenID:       receiptTrade.TokenID,
							Side:          string(receiptTrade.Side),
							AmountUnits:   amount.String(),
							CopyBps:       parsed.copyBps,
							ExactAsLeader: parsed.exactAsLeader,
							Price:         orderRes.Price,
							TickSize:      orderRes.TickSize,
							BlockMs:       blockMs,
							RxMs:          rxMs,
							RxLagMs:       rxLagMs,
							OrderType:     string(parsed.orderType),
							EnableTrading: false,
							Ok:            true,
							UptimeMs:      time.Since(runStartedAt).Milliseconds(),
						})
						advanceCheckpoint(parsed, ckpt, lastFill)
						continue
					}

					resp, _, err := clobClient.PostSignedOrder(ctx, orderRes.SignedOrder, parsed.orderType, false, parsed.useServerTime)
					if err != nil {
						logCopyEvent(tradeLog, copyLogEvent{
							TsMs:          time.Now().UnixMilli(),
							Event:         "trade",
							Mode:          "live",
							Leader:        leader.Hex(),
							TxHash:        repFill.TxHash.Hex(),
							LogIndex:      uint64(repFill.LogIndex),
							TokenID:       receiptTrade.TokenID,
							Side:          string(receiptTrade.Side),
							AmountUnits:   amount.String(),
							CopyBps:       parsed.copyBps,
							ExactAsLeader: parsed.exactAsLeader,
							Price:         orderRes.Price,
							TickSize:      orderRes.TickSize,
							BlockMs:       blockMs,
							RxMs:          rxMs,
							RxLagMs:       rxLagMs,
							OrderType:     string(parsed.orderType),
							EnableTrading: true,
							Ok:            false,
							Err:           err.Error(),
							UptimeMs:      time.Since(runStartedAt).Milliseconds(),
						})
						log.Printf("[warn] post order failed: %v", err)
						advanceCheckpoint(parsed, ckpt, lastFill)
						continue
					}
					log.Printf("[trade] tx=%s idx=%d blockMs=%d rxMs=%d rxLagMs=%d → resp=%v", repFill.TxHash.Hex(), repFill.LogIndex, blockMs, rxMs, rxLagMs, resp)
					logCopyEvent(tradeLog, copyLogEvent{
						TsMs:          time.Now().UnixMilli(),
						Event:         "trade",
						Mode:          "live",
						Leader:        leader.Hex(),
						TxHash:        repFill.TxHash.Hex(),
						LogIndex:      uint64(repFill.LogIndex),
						TokenID:       receiptTrade.TokenID,
						Side:          string(receiptTrade.Side),
						AmountUnits:   amount.String(),
						CopyBps:       parsed.copyBps,
						ExactAsLeader: parsed.exactAsLeader,
						Price:         orderRes.Price,
						TickSize:      orderRes.TickSize,
						BlockMs:       blockMs,
						RxMs:          rxMs,
						RxLagMs:       rxLagMs,
						OrderType:     string(parsed.orderType),
						EnableTrading: true,
						Ok:            true,
						Resp:          resp,
						UptimeMs:      time.Since(runStartedAt).Milliseconds(),
					})
					advanceCheckpoint(parsed, ckpt, lastFill)
					continue
				}

				if receiptErr != nil {
					log.Printf("[warn] receipt infer failed tx=%s leader=%s (falling back to OrderFilled): %v", run.hash.Hex(), leader.Hex(), receiptErr)
				}
			} else {
				log.Printf("[warn] tx=%s has fills for multiple leaders; skipping receipt inference", run.hash.Hex())
			}

			// Fallback: process each fill using OrderFilled event semantics.
			for _, fill := range run.fills {
				if fill == nil || cursor.beforeOrEqual(fill) {
					continue
				}

				leaderFill := fill.Taker
				if _, ok := parsed.leaderSet[leaderFill]; !ok {
					continue
				}
				trade, err := copytrade.ComputeLeaderTakerTrade(fill, leaderFill, collateralUSDC)
				if err != nil {
					log.Printf("[warn] skip fill tx=%s idx=%d: %v", fill.TxHash.Hex(), fill.LogIndex, err)
					advanceCheckpoint(parsed, ckpt, fill)
					continue
				}
				if trade == nil {
					continue
				}

				var amount *big.Int
				if parsed.exactAsLeader {
					amount = new(big.Int).Set(trade.AmountUnits)
				} else {
					amount = scaleByBps(trade.AmountUnits, parsed.copyBps)
				}
				if amount.Sign() <= 0 {
					log.Printf("[warn] amount scaled to 0, skip tx=%s idx=%d", fill.TxHash.Hex(), fill.LogIndex)
					advanceCheckpoint(parsed, ckpt, fill)
					continue
				}

				salt := deterministicSalt(fill, clobClient.SignerAddress())
				saltGen := func() int64 { return salt }

				var orderRes *clob.OrderResult
				if parsed.exactAsLeader {
					if fill.MakerAmountFilled == nil || fill.MakerAmountFilled.Sign() <= 0 || fill.TakerAmountFilled == nil || fill.TakerAmountFilled.Sign() <= 0 {
						log.Printf("[warn] skip exact-as-leader (missing filled amounts) tx=%s idx=%d", fill.TxHash.Hex(), fill.LogIndex)
						advanceCheckpoint(parsed, ckpt, fill)
						continue
					}
					var makerAmount, takerAmount *big.Int
					switch trade.Side {
					case clob.SideBuy:
						makerAmount = new(big.Int).Set(fill.TakerAmountFilled) // collateral
						takerAmount = new(big.Int).Set(fill.MakerAmountFilled) // shares
					case clob.SideSell:
						makerAmount = new(big.Int).Set(fill.TakerAmountFilled) // shares
						takerAmount = new(big.Int).Set(fill.MakerAmountFilled) // collateral
					default:
						log.Printf("[warn] unsupported side %q tx=%s idx=%d", trade.Side, fill.TxHash.Hex(), fill.LogIndex)
						advanceCheckpoint(parsed, ckpt, fill)
						continue
					}
					orderRes, err = clobClient.CreateSignedExactOrder(ctx, trade.TokenID, trade.Side, makerAmount, takerAmount, saltGen)
				} else {
					orderRes, err = clobClient.CreateSignedMarketOrder(ctx, trade.TokenID, trade.Side, amount, parsed.orderType, parsed.useServerTime, saltGen)
				}
				if err != nil {
					log.Printf("[warn] create order failed (token=%s side=%s amount=%s): %v", trade.TokenID, trade.Side, amount.String(), err)
					advanceCheckpoint(parsed, ckpt, fill)
					continue
				}

				body, _ := clobClient.BuildPostOrderBody(orderRes.SignedOrder, parsed.orderType, false)
				rxLagMs := int64(-1)
				if blockMs >= 0 && fill.ReceivedAtMs > 0 {
					rxLagMs = fill.ReceivedAtMs - blockMs
				}
				if !parsed.enableTrading {
					mode := "scaled-market"
					if parsed.exactAsLeader {
						mode = "exact-as-leader"
					}
					log.Printf(
						"[dry-run] mode=%s fill tx=%s idx=%d leader=%s evMaker=%s evTaker=%s evMakerAssetId=%s evTakerAssetId=%s evMakerFilled=%s evTakerFilled=%s → copy token=%s side=%s amount=%s copyBps=%d clobMaker=%s clobSigner=%s clobSigType=%s clobMakerAmt=%s clobTakerAmt=%s price=%s tick=%s blockMs=%d rxMs=%d rxLagMs=%d payload=%s",
						mode,
						fill.TxHash.Hex(),
						fill.LogIndex,
						leaderFill.Hex(),
						fill.Maker.Hex(),
						fill.Taker.Hex(),
						bigString(fill.MakerAssetId),
						bigString(fill.TakerAssetId),
						bigString(fill.MakerAmountFilled),
						bigString(fill.TakerAmountFilled),
						trade.TokenID,
						trade.Side,
						amount.String(),
						parsed.copyBps,
						orderRes.SignedOrder.Maker.Hex(),
						orderRes.SignedOrder.Signer.Hex(),
						bigString(orderRes.SignedOrder.SignatureType),
						bigString(orderRes.SignedOrder.MakerAmount),
						bigString(orderRes.SignedOrder.TakerAmount),
						orderRes.Price,
						orderRes.TickSize,
						blockMs,
						fill.ReceivedAtMs,
						rxLagMs,
						strings.TrimSpace(string(body)),
					)
					logCopyEvent(tradeLog, copyLogEvent{
						TsMs:          time.Now().UnixMilli(),
						Event:         "dry_run",
						Mode:          "dry",
						Leader:        leaderFill.Hex(),
						TxHash:        fill.TxHash.Hex(),
						LogIndex:      uint64(fill.LogIndex),
						TokenID:       trade.TokenID,
						Side:          string(trade.Side),
						AmountUnits:   amount.String(),
						CopyBps:       parsed.copyBps,
						ExactAsLeader: parsed.exactAsLeader,
						Price:         orderRes.Price,
						TickSize:      orderRes.TickSize,
						BlockMs:       blockMs,
						RxMs:          fill.ReceivedAtMs,
						RxLagMs:       rxLagMs,
						OrderType:     string(parsed.orderType),
						EnableTrading: false,
						Ok:            true,
						UptimeMs:      time.Since(runStartedAt).Milliseconds(),
					})
					advanceCheckpoint(parsed, ckpt, fill)
					continue
				}

				resp, _, err := clobClient.PostSignedOrder(ctx, orderRes.SignedOrder, parsed.orderType, false, parsed.useServerTime)
				if err != nil {
					logCopyEvent(tradeLog, copyLogEvent{
						TsMs:          time.Now().UnixMilli(),
						Event:         "trade",
						Mode:          "live",
						Leader:        leaderFill.Hex(),
						TxHash:        fill.TxHash.Hex(),
						LogIndex:      uint64(fill.LogIndex),
						TokenID:       trade.TokenID,
						Side:          string(trade.Side),
						AmountUnits:   amount.String(),
						CopyBps:       parsed.copyBps,
						ExactAsLeader: parsed.exactAsLeader,
						Price:         orderRes.Price,
						TickSize:      orderRes.TickSize,
						BlockMs:       blockMs,
						RxMs:          fill.ReceivedAtMs,
						RxLagMs:       rxLagMs,
						OrderType:     string(parsed.orderType),
						EnableTrading: true,
						Ok:            false,
						Err:           err.Error(),
						UptimeMs:      time.Since(runStartedAt).Milliseconds(),
					})
					log.Printf("[warn] post order failed: %v", err)
					advanceCheckpoint(parsed, ckpt, fill)
					continue
				}
				log.Printf("[trade] fill tx=%s idx=%d blockMs=%d rxMs=%d rxLagMs=%d → resp=%v", fill.TxHash.Hex(), fill.LogIndex, blockMs, fill.ReceivedAtMs, rxLagMs, resp)
				logCopyEvent(tradeLog, copyLogEvent{
					TsMs:          time.Now().UnixMilli(),
					Event:         "trade",
					Mode:          "live",
					Leader:        leaderFill.Hex(),
					TxHash:        fill.TxHash.Hex(),
					LogIndex:      uint64(fill.LogIndex),
					TokenID:       trade.TokenID,
					Side:          string(trade.Side),
					AmountUnits:   amount.String(),
					CopyBps:       parsed.copyBps,
					ExactAsLeader: parsed.exactAsLeader,
					Price:         orderRes.Price,
					TickSize:      orderRes.TickSize,
					BlockMs:       blockMs,
					RxMs:          fill.ReceivedAtMs,
					RxLagMs:       rxLagMs,
					OrderType:     string(parsed.orderType),
					EnableTrading: true,
					Ok:            true,
					Resp:          resp,
					UptimeMs:      time.Since(runStartedAt).Milliseconds(),
				})
				advanceCheckpoint(parsed, ckpt, fill)
			}
		}

		delete(q.byBlock, bn)
	}

	return nil
}

func advanceCheckpoint(parsed args, ckpt *state.Checkpoint, fill *polygonwatch.FillEvent) {
	if ckpt == nil || fill == nil {
		return
	}
	// Never move the checkpoint backwards (important if we ever process out of order).
	if ckpt.LastProcessedBlock > fill.BlockNumber {
		return
	}
	if ckpt.LastProcessedBlock == fill.BlockNumber && ckpt.LastProcessedLogIndex >= fill.LogIndex {
		return
	}

	leaders := leaderHexes(parsed.leadersCanonical)
	ckpt.ChainID = 137
	ckpt.LeaderAddress = leaders[0]
	ckpt.LeaderAddresses = leaders
	ckpt.ExchangeAddress = exchangeAddresses[0].Hex()
	ckpt.LastProcessedBlock = fill.BlockNumber
	ckpt.LastProcessedLogIndex = fill.LogIndex
	if err := state.SaveCheckpoint(parsed.checkpointFile, *ckpt); err != nil {
		log.Printf("[warn] checkpoint save failed: %v", err)
	}
}

func scaleByBps(amount *big.Int, bps int64) *big.Int {
	if amount == nil {
		return new(big.Int)
	}
	out := new(big.Int).Mul(amount, big.NewInt(bps))
	out.Div(out, big.NewInt(10000))
	return out
}

func bigString(v *big.Int) string {
	if v == nil {
		return "<nil>"
	}
	return v.String()
}

func deterministicSalt(fill *polygonwatch.FillEvent, signer common.Address) int64 {
	var buf [8 + 32 + 20]byte
	copy(buf[0:32], fill.TxHash.Bytes())
	binary.BigEndian.PutUint64(buf[32:40], uint64(fill.LogIndex))
	copy(buf[40:60], signer.Bytes())

	h := crypto.Keccak256Hash(buf[:])
	u := binary.BigEndian.Uint64(h[:8])
	u &= (1<<63 - 1)
	if u == 0 {
		u = 1
	}
	return int64(u)
}

func parseEthGetLogsRangeLimit(err error) (uint64, bool) {
	if err == nil {
		return 0, false
	}
	const marker = "limited to a "
	s := err.Error()
	idx := strings.Index(s, marker)
	if idx < 0 {
		return 0, false
	}
	rest := s[idx+len(marker):]
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	if j == 0 {
		return 0, false
	}
	limit, parseErr := strconv.ParseUint(rest[:j], 10, 64)
	if parseErr != nil {
		return 0, false
	}
	return limit, true
}
