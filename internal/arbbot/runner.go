package arbbot

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/jsonl"
	"poly-gocopy/internal/polygonutil"
)

type strategyLogger func(args)
type backgroundFunc func(ctx context.Context)

func runStrategy(parsed args, strategyName string, buyStep buyStepFunc, logStrategy strategyLogger, background backgroundFunc) {
	pk, ephemeral, err := parseOrGeneratePrivateKey(parsed.privateKeyHex)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}
	if ephemeral && !parsed.enableTrading {
		log.Printf("[info] no private key provided; using ephemeral key for dry-run")
	}

	signer := crypto.PubkeyToAddress(pk.PublicKey)
	funder := parsed.funder
	if (funder == common.Address{}) {
		funder = signer
	}

	if parsed.capSplitBalance {
		if parsed.capAExplicit || parsed.capBExplicit {
			log.Printf("[cfg] cap-split-balance enabled, but --cap-a/--cap-b explicitly set; ignoring auto caps")
		} else {
			if parsed.privateKeyHex == "" && (parsed.funder == common.Address{}) {
				log.Fatalf("[fatal] cap-split-balance requires FUNDER/CLOB_FUNDER or PRIVATE_KEY (to infer signer wallet)")
			}
			rpcURL, err := polygonutil.RPCURLFromEnv()
			if err != nil {
				log.Fatalf("[fatal] cap-split-balance: %v", err)
			}
			balCtx, balCancel := context.WithTimeout(context.Background(), 12*time.Second)
			usdcMicros, err := polygonutil.USDCTokenBalanceMicros(balCtx, rpcURL, funder)
			balCancel()
			if err != nil {
				log.Fatalf("[fatal] cap-split-balance: %v", err)
			}
			half := usdcMicros / 2
			if half == 0 {
				log.Fatalf("[fatal] cap-split-balance: USDC balance too small (balance=%s)", formatMicros(usdcMicros))
			}
			parsed.capAMicros = half
			parsed.capBMicros = half
			log.Printf(
				"[cfg] cap-split-balance: funder=%s usdc_balance=%s cap_a=%s cap_b=%s",
				funder.Hex(),
				formatMicros(usdcMicros),
				formatMicros(parsed.capAMicros),
				formatMicros(parsed.capBMicros),
			)
		}
	}

	runStartedAt := time.Now()
	tradeLog := jsonl.New(parsed.outFile)
	if tradeLog != nil {
		log.Printf("Trade log: %s (JSONL)", parsed.outFile)
		defer func() {
			if err := tradeLog.Close(); err != nil {
				log.Printf("[warn] trade log close: %v", err)
			}
		}()
		logArbEvent(tradeLog, arbLogEvent{
			TsMs:          time.Now().UnixMilli(),
			Event:         "start",
			Mode:          arbMode(parsed.enableTrading),
			Source:        parsed.source,
			EventSlugs:    append([]string(nil), parsed.eventSlugs...),
			TokenA:        parsed.tokenA,
			TokenB:        parsed.tokenB,
			CapAMicros:    parsed.capAMicros,
			CapBMicros:    parsed.capBMicros,
			OrderType:     string(parsed.orderType),
			EnableTrading: parsed.enableTrading,
			UptimeMs:      0,
		})
	}

	log.Printf("Polymarket arbitrage bot (strategy=%s)", strategyName)
	if len(parsed.eventSlugs) > 0 || strings.TrimSpace(parsed.eventSlugsFile) != "" {
		if len(parsed.eventSlugs) == 1 {
			log.Printf("Event slug: %s (auto-switch)", parsed.eventSlugs[0])
		} else if len(parsed.eventSlugs) > 1 {
			log.Printf("Event slugs: %s (auto-switch)", strings.Join(parsed.eventSlugs, ", "))
		} else {
			log.Printf("Event slugs: (none yet) (file=%s) (auto-switch)", parsed.eventSlugsFile)
		}
		log.Printf("Gamma: %s", parsed.gammaURL)
		log.Printf("Timezone: %s", parsed.eventTZ)
	} else {
		log.Printf("Token A: %s", parsed.tokenA)
		log.Printf("Token B: %s", parsed.tokenB)
	}
	log.Printf("Caps: A=%s B=%s", formatMicros(parsed.capAMicros), formatMicros(parsed.capBMicros))
	if logStrategy != nil {
		logStrategy(parsed)
	}
	log.Printf("Source: %s", parsed.source)
	switch parsed.source {
	case "poll":
		log.Printf("Poll: %s", parsed.pollInterval)
	case "rtds":
		log.Printf("RTDS: %s (ping=%s min_eval=%s)", parsed.rtdsURL, parsed.rtdsPingInterval, parsed.rtdsMinInterval)
	}
	log.Printf("Dry-run: %v", !parsed.enableTrading)
	if parsed.enableSell {
		log.Printf(
			"Sell: enabled (every=%s target>=%0.2f%% order=%s)",
			parsed.sellEvery,
			float64(parsed.rangeProfitTargetBps)/100.0,
			parsed.sellOrderType,
		)
	} else {
		log.Printf("Sell: disabled (strategy holds positions)")
	}
	if parsed.nextSlugLast > 0 {
		log.Printf("Next slug: enabled (start <=%s remaining)", parsed.nextSlugLast)
	} else {
		log.Printf("Next slug: disabled")
	}

	clobClient, err := clob.NewClient(parsed.clobHost, 137, pk, parsed.funder, parsed.signatureType)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if background != nil {
		go background(ctx)
	}

	reloadCh := make(chan struct{}, 1)
	sigCh := make(chan os.Signal, 8)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigCh:
				switch sig {
				case syscall.SIGHUP:
					select {
					case reloadCh <- struct{}{}:
					default:
					}
				default:
					log.Printf("Shutting down…")
					cancel()
					return
				}
			}
		}
	}()

	if parsed.enableTrading {
		if parsed.apiKey != "" && parsed.apiSecret != "" && parsed.apiPassphrase != "" {
			clobClient.SetApiCreds(clob.ApiKeyCreds{Key: parsed.apiKey, Secret: parsed.apiSecret, Passphrase: parsed.apiPassphrase})
		} else {
			creds, err := clobClient.CreateOrDeriveApiKey(ctx, parsed.apiNonce, parsed.useServerTime)
			if err != nil {
				log.Fatalf("[fatal] failed to create/derive api key: %v", err)
			}
			clobClient.SetApiCreds(creds)
			log.Printf("CLOB API creds ready (key=%s…)", safePrefix(creds.Key, 8))
		}
	}

	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()>>1)))
	var rngMu sync.Mutex
	saltGen := func() int64 {
		rngMu.Lock()
		defer rngMu.Unlock()
		return int64(rng.Uint64() & 0x7fffffffffffffff)
	}

	log.Printf("Listening…")
	if len(parsed.eventSlugs) > 0 || strings.TrimSpace(parsed.eventSlugsFile) != "" {
		if len(parsed.eventSlugs) == 0 && strings.TrimSpace(parsed.eventSlugsFile) != "" {
			log.Printf("[cfg] no event slugs loaded yet (file=%s); send SIGHUP after editing to reload", parsed.eventSlugsFile)
		}
		if err := runAutoSwitchManager(ctx, cancel, reloadCh, clobClient, parsed, saltGen, tradeLog, runStartedAt, buyStep); err != nil {
			log.Fatalf("[fatal] %v", err)
		}
		return
	}

	st := botState{
		a: sideState{
			tokenID:   parsed.tokenA,
			capMicros: parsed.capAMicros,
		},
		b: sideState{
			tokenID:   parsed.tokenB,
			capMicros: parsed.capBMicros,
		},
	}
	if tradeLog != nil {
		defer func() {
			logArbEvent(tradeLog, arbLogEvent{
				TsMs:         time.Now().UnixMilli(),
				Event:        "summary",
				Mode:         arbMode(parsed.enableTrading),
				Source:       parsed.source,
				EventSlug:    parsed.eventSlug,
				TokenA:       parsed.tokenA,
				TokenB:       parsed.tokenB,
				CapAMicros:   st.a.capMicros,
				CapBMicros:   st.b.capMicros,
				SpentAMicros: st.a.spentMicros,
				SpentBMicros: st.b.spentMicros,
				UptimeMs:     time.Since(runStartedAt).Milliseconds(),
			})
		}()
	}

	switch parsed.source {
	case "poll":
		runPollLoop(ctx, clobClient, parsed, &st, saltGen, tradeLog, runStartedAt, "", time.Time{}, time.Time{}, buyStep)
	case "rtds":
		runRTDSLoop(ctx, clobClient, parsed, &st, saltGen, tradeLog, runStartedAt, "", time.Time{}, time.Time{}, buyStep)
	default:
		log.Fatalf("[fatal] unknown source %q", parsed.source)
	}
}
