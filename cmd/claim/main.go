package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	orderconfig "github.com/polymarket/go-order-utils/pkg/config"
	exchangefees "github.com/polymarket/go-order-utils/pkg/contracts/exchange-fees"

	"poly-gocopy/internal/dataapi"
	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/polygonutil"
)

const (
	defaultClaimLimit      = 500
	maxClaimOffset         = 10000
	defaultSafeWaitTimeout = 3 * time.Minute
)

const ctfABIJSON = `[
  {"inputs":[
    {"internalType":"address","name":"collateralToken","type":"address"},
    {"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},
    {"internalType":"bytes32","name":"conditionId","type":"bytes32"},
    {"internalType":"uint256[]","name":"indexSets","type":"uint256[]"}
  ],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
]`

const safeABIJSON = `[
  {"inputs":[],"name":"nonce","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
  {"inputs":[
    {"internalType":"address","name":"to","type":"address"},
    {"internalType":"uint256","name":"value","type":"uint256"},
    {"internalType":"bytes","name":"data","type":"bytes"},
    {"internalType":"uint8","name":"operation","type":"uint8"},
    {"internalType":"uint256","name":"safeTxGas","type":"uint256"},
    {"internalType":"uint256","name":"baseGas","type":"uint256"},
    {"internalType":"uint256","name":"gasPrice","type":"uint256"},
    {"internalType":"address","name":"gasToken","type":"address"},
    {"internalType":"address","name":"refundReceiver","type":"address"},
    {"internalType":"uint256","name":"nonce","type":"uint256"}
  ],"name":"getTransactionHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},
  {"inputs":[
    {"internalType":"address","name":"to","type":"address"},
    {"internalType":"uint256","name":"value","type":"uint256"},
    {"internalType":"bytes","name":"data","type":"bytes"},
    {"internalType":"uint8","name":"operation","type":"uint8"},
    {"internalType":"uint256","name":"safeTxGas","type":"uint256"},
    {"internalType":"uint256","name":"baseGas","type":"uint256"},
    {"internalType":"uint256","name":"gasPrice","type":"uint256"},
    {"internalType":"address","name":"gasToken","type":"address"},
    {"internalType":"address","name":"refundReceiver","type":"address"},
    {"internalType":"bytes","name":"signatures","type":"bytes"}
  ],"name":"execTransaction","outputs":[{"internalType":"bool","name":"success","type":"bool"}],"stateMutability":"payable","type":"function"},
  {"inputs":[],"name":"getOwners","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},
  {"inputs":[],"name":"getThreshold","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]`

type config struct {
	dataURL       string
	interval      time.Duration
	sizeThreshold float64
	limit         int
	enableClaims  bool

	signatureType int
	privateKey    *ecdsa.PrivateKey
	signer        common.Address
	funder        common.Address
}

type claimItem struct {
	ConditionID common.Hash
	IndexSets   []*big.Int
	Positions   []dataapi.Position
}

func main() {
	log.SetFlags(0)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	baseCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.interval <= 0 {
		if err := runOnce(baseCtx, cfg); err != nil {
			log.Fatalf("[fatal] %v", err)
		}
		return
	}

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	for {
		if err := runOnce(baseCtx, cfg); err != nil {
			log.Printf("[warn] claim run failed: %v", err)
		}
		select {
		case <-baseCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func loadConfig() (config, error) {
	var cfg config

	var intervalFlag string
	var sizeThresholdFlag float64
	var limitFlag int
	var enableClaimsFlag bool

	flag.StringVar(&intervalFlag, "every", "", "Claim interval (e.g. 30m). Empty = run once (default).")
	flag.Float64Var(&sizeThresholdFlag, "size-threshold", -1, "Minimum redeemable size (default from CLAIM_SIZE_THRESHOLD or 0).")
	flag.IntVar(&limitFlag, "limit", 0, "Max positions per page (default from CLAIM_LIMIT or 500).")
	flag.BoolVar(&enableClaimsFlag, "enable-claims", false, "Send claim transactions (default false; set ENABLE_CLAIMS).")
	flag.Parse()

	dataURL := strings.TrimSpace(os.Getenv("DATA_API_URL"))
	if dataURL == "" {
		dataURL = dataapi.DefaultURL
	}

	interval := time.Duration(0)
	if strings.TrimSpace(intervalFlag) != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(intervalFlag))
		if err != nil {
			return cfg, fmt.Errorf("invalid --every duration %q: %w", intervalFlag, err)
		}
		interval = parsed
	} else if env := strings.TrimSpace(os.Getenv("CLAIM_EVERY")); env != "" {
		parsed, err := time.ParseDuration(env)
		if err != nil {
			return cfg, fmt.Errorf("invalid CLAIM_EVERY %q: %w", env, err)
		}
		interval = parsed
	}

	sizeThreshold := 0.0
	if sizeThresholdFlag >= 0 {
		sizeThreshold = sizeThresholdFlag
	} else if env := strings.TrimSpace(os.Getenv("CLAIM_SIZE_THRESHOLD")); env != "" {
		v, err := strconv.ParseFloat(env, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid CLAIM_SIZE_THRESHOLD %q: %w", env, err)
		}
		sizeThreshold = v
	}

	limit := defaultClaimLimit
	if limitFlag > 0 {
		limit = limitFlag
	} else if env := strings.TrimSpace(os.Getenv("CLAIM_LIMIT")); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil {
			return cfg, fmt.Errorf("invalid CLAIM_LIMIT %q: %w", env, err)
		}
		if v > 0 {
			limit = v
		}
	}
	if limit <= 0 {
		limit = defaultClaimLimit
	}
	if limit > defaultClaimLimit {
		limit = defaultClaimLimit
	}

	enableClaims := enableClaimsFlag
	if !enableClaims {
		if env := strings.TrimSpace(os.Getenv("ENABLE_CLAIMS")); env != "" {
			v, err := strconv.ParseBool(env)
			if err != nil {
				return cfg, fmt.Errorf("invalid ENABLE_CLAIMS %q: %w", env, err)
			}
			enableClaims = v
		} else if env := strings.TrimSpace(os.Getenv("ENABLE_TRADING")); env != "" {
			v, err := strconv.ParseBool(env)
			if err != nil {
				return cfg, fmt.Errorf("invalid ENABLE_TRADING %q: %w", env, err)
			}
			enableClaims = v
		}
	}

	signatureType := 0
	if env := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_SIGNATURE_TYPE"), os.Getenv("SIGNATURE_TYPE"))); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil {
			return cfg, fmt.Errorf("invalid signature type env %q: %w", env, err)
		}
		signatureType = v
	}

	var signer common.Address
	var privateKey *ecdsa.PrivateKey
	if pkHex := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_PRIVATE_KEY"), os.Getenv("PRIVATE_KEY"))); pkHex != "" {
		pkHex = strings.TrimSpace(strings.TrimPrefix(pkHex, "0x"))
		pk, err := crypto.HexToECDSA(pkHex)
		if err != nil {
			return cfg, fmt.Errorf("invalid PRIVATE_KEY: %w", err)
		}
		privateKey = pk
		signer = crypto.PubkeyToAddress(pk.PublicKey)
	}

	var funder common.Address
	if envFunder := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_FUNDER"), os.Getenv("FUNDER"))); envFunder != "" {
		if !common.IsHexAddress(envFunder) {
			return cfg, fmt.Errorf("invalid FUNDER/CLOB_FUNDER %q", envFunder)
		}
		funder = common.HexToAddress(envFunder)
	} else if (signer != common.Address{}) {
		funder = signer
	}

	if funder == (common.Address{}) {
		return cfg, fmt.Errorf("funder required: set FUNDER/CLOB_FUNDER or PRIVATE_KEY")
	}
	if enableClaims && privateKey == nil {
		return cfg, fmt.Errorf("private key required to execute claims (set PRIVATE_KEY/CLOB_PRIVATE_KEY)")
	}

	cfg = config{
		dataURL:       dataURL,
		interval:      interval,
		sizeThreshold: sizeThreshold,
		limit:         limit,
		enableClaims:  enableClaims,
		signatureType: signatureType,
		privateKey:    privateKey,
		signer:        signer,
		funder:        funder,
	}
	return cfg, nil
}

func runOnce(ctx context.Context, cfg config) error {
	dataClient, err := dataapi.NewClient(cfg.dataURL)
	if err != nil {
		return err
	}

	positions, err := fetchRedeemablePositions(ctx, dataClient, cfg.funder.Hex(), cfg.sizeThreshold, cfg.limit)
	if err != nil {
		return err
	}

	items, skippedNeg, skippedInvalid := buildClaimItems(positions)
	if len(items) == 0 {
		log.Printf("[claim] user=%s redeemable=0 skipped_neg=%d skipped_invalid=%d", cfg.funder.Hex(), skippedNeg, skippedInvalid)
		return nil
	}

	log.Printf("[claim] user=%s redeemable=%d buckets=%d skipped_neg=%d skipped_invalid=%d", cfg.funder.Hex(), len(positions), len(items), skippedNeg, skippedInvalid)
	for _, item := range items {
		title, outcome, size := summarizePositions(item.Positions)
		log.Printf("[claim] ready condition=%s indexSets=%s title=%q outcome=%q size=%.6f", item.ConditionID.Hex(), formatIndexSets(item.IndexSets), title, outcome, size)
	}

	if !cfg.enableClaims {
		log.Printf("[claim] dry-run: set ENABLE_CLAIMS=true (or --enable-claims) to submit transactions")
		return nil
	}

	rpcURL, err := polygonutil.RPCURLFromEnv()
	if err != nil {
		return err
	}
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("dial polygon rpc: %w", err)
	}
	defer client.Close()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetch chain id: %w", err)
	}

	contracts, err := orderconfig.GetContracts(chainID.Int64())
	if err != nil {
		return fmt.Errorf("contracts for chain %d: %w", chainID.Int64(), err)
	}

	collateral, ctfAddr, err := resolveCollateralAndCTF(ctx, client, contracts)
	if err != nil {
		return err
	}

	ctfABI, err := abi.JSON(strings.NewReader(ctfABIJSON))
	if err != nil {
		return fmt.Errorf("ctf abi parse: %w", err)
	}

	parentCollectionID := [32]byte{}
	switch cfg.signatureType {
	case 0:
		for _, item := range items {
			txHash, err := redeemEOA(ctx, client, chainID, cfg.privateKey, ctfABI, ctfAddr, collateral, parentCollectionID, item)
			if err != nil {
				log.Printf("[warn] redeem condition=%s failed: %v", item.ConditionID.Hex(), err)
				continue
			}
			log.Printf("[claim] sent tx=%s condition=%s", txHash.Hex(), item.ConditionID.Hex())
		}
	case 2:
		if err := redeemSafe(ctx, client, chainID, cfg, ctfABI, ctfAddr, collateral, parentCollectionID, items); err != nil {
			return err
		}
	default:
		return fmt.Errorf("signature type %d not supported for claims (use 0=EOA or 2=POLY_GNOSIS_SAFE)", cfg.signatureType)
	}

	return nil
}

func fetchRedeemablePositions(ctx context.Context, client *dataapi.Client, user string, sizeThreshold float64, limit int) ([]dataapi.Position, error) {
	redeemable := true
	threshold := sizeThreshold
	offset := 0
	out := make([]dataapi.Position, 0, limit)

	for {
		batch, err := client.GetPositions(ctx, dataapi.PositionsParams{
			User:          user,
			Redeemable:    &redeemable,
			SizeThreshold: &threshold,
			Limit:         limit,
			Offset:        offset,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if len(batch) < limit {
			break
		}
		offset += len(batch)
		if offset >= maxClaimOffset {
			break
		}
	}
	return out, nil
}

type claimBucket struct {
	item         claimItem
	negativeRisk bool
}

func buildClaimItems(positions []dataapi.Position) ([]claimItem, int, int) {
	buckets := make(map[string]*claimBucket)
	skippedNeg := 0
	skippedInvalid := 0

	for _, pos := range positions {
		cond, err := parseConditionID(pos.ConditionID)
		if err != nil {
			skippedInvalid++
			log.Printf("[warn] skip position: invalid conditionId %q", pos.ConditionID)
			continue
		}
		indexSet, err := outcomeIndexToSet(pos.OutcomeIndex)
		if err != nil {
			skippedInvalid++
			log.Printf("[warn] skip position: invalid outcomeIndex=%d condition=%s", pos.OutcomeIndex, cond.Hex())
			continue
		}
		key := cond.Hex()
		bucket := buckets[key]
		if bucket == nil {
			bucket = &claimBucket{
				item: claimItem{
					ConditionID: cond,
					IndexSets:   []*big.Int{},
					Positions:   []dataapi.Position{},
				},
			}
			buckets[key] = bucket
		}
		if pos.NegativeRisk {
			bucket.negativeRisk = true
			bucket.item.IndexSets = nil
			bucket.item.Positions = nil
			skippedNeg++
			continue
		}
		if bucket.negativeRisk {
			skippedNeg++
			continue
		}
		if !containsIndexSet(bucket.item.IndexSets, indexSet) {
			bucket.item.IndexSets = append(bucket.item.IndexSets, indexSet)
		}
		bucket.item.Positions = append(bucket.item.Positions, pos)
	}

	items := make([]claimItem, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket.negativeRisk {
			continue
		}
		if len(bucket.item.IndexSets) == 0 || len(bucket.item.Positions) == 0 {
			continue
		}
		sort.Slice(bucket.item.IndexSets, func(i, j int) bool {
			return bucket.item.IndexSets[i].Cmp(bucket.item.IndexSets[j]) < 0
		})
		items = append(items, bucket.item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].ConditionID.Hex() < items[j].ConditionID.Hex()
	})
	return items, skippedNeg, skippedInvalid
}

func summarizePositions(positions []dataapi.Position) (string, string, float64) {
	if len(positions) == 0 {
		return "", "", 0
	}
	title := strings.TrimSpace(positions[0].Title)
	outcome := strings.TrimSpace(positions[0].Outcome)
	var total float64
	for _, p := range positions {
		total += p.Size
		if title == "" {
			title = strings.TrimSpace(p.Title)
		}
		if outcome == "" {
			outcome = strings.TrimSpace(p.Outcome)
		}
	}
	return title, outcome, total
}

func resolveCollateralAndCTF(ctx context.Context, client *ethclient.Client, contracts *orderconfig.Contracts) (common.Address, common.Address, error) {
	collateral := contracts.Collateral
	ctfAddr := contracts.Conditional

	exchangeFees, err := exchangefees.NewExchangeFees(contracts.FeeModule, client)
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("exchange fees binding: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	if onchainCollateral, err := exchangeFees.Collateral(&bind.CallOpts{Context: callCtx}); err != nil {
		log.Printf("[warn] exchange fees collateral lookup failed: %v; using config=%s", err, collateral.Hex())
	} else if onchainCollateral != (common.Address{}) {
		collateral = onchainCollateral
	}

	if onchainCTF, err := exchangeFees.Ctf(&bind.CallOpts{Context: callCtx}); err == nil && onchainCTF != (common.Address{}) {
		if ctfAddr != onchainCTF {
			log.Printf("[warn] ctf address mismatch config=%s exchange=%s; using exchange value", ctfAddr.Hex(), onchainCTF.Hex())
			ctfAddr = onchainCTF
		}
	} else if err != nil {
		log.Printf("[warn] exchange fees ctf lookup failed: %v; using config=%s", err, ctfAddr.Hex())
	}

	if collateral == (common.Address{}) {
		return common.Address{}, common.Address{}, fmt.Errorf("collateral address missing after resolution")
	}
	if ctfAddr == (common.Address{}) {
		return common.Address{}, common.Address{}, fmt.Errorf("ctf address missing after resolution")
	}
	return collateral, ctfAddr, nil
}

func redeemEOA(ctx context.Context, client *ethclient.Client, chainID *big.Int, pk *ecdsa.PrivateKey, ctfABI abi.ABI, ctfAddr, collateral common.Address, parentCollectionID [32]byte, item claimItem) (common.Hash, error) {
	opts, err := bind.NewKeyedTransactorWithChainID(pk, chainID)
	if err != nil {
		return common.Hash{}, err
	}
	opts.Context = ctx

	contract := bind.NewBoundContract(ctfAddr, ctfABI, client, client, client)
	tx, err := contract.Transact(opts, "redeemPositions", collateral, parentCollectionID, item.ConditionID, item.IndexSets)
	if err != nil {
		return common.Hash{}, err
	}
	return tx.Hash(), nil
}

func redeemSafe(ctx context.Context, client *ethclient.Client, chainID *big.Int, cfg config, ctfABI abi.ABI, ctfAddr, collateral common.Address, parentCollectionID [32]byte, items []claimItem) error {
	safeABI, err := abi.JSON(strings.NewReader(safeABIJSON))
	if err != nil {
		return fmt.Errorf("safe abi parse: %w", err)
	}

	threshold, owners, err := fetchSafeMeta(ctx, client, safeABI, cfg.funder)
	if err != nil {
		return err
	}
	if threshold > 1 {
		return fmt.Errorf("safe threshold=%d not supported (needs 1-of-1)", threshold)
	}
	if cfg.signer != (common.Address{}) && len(owners) > 0 && !containsAddress(owners, cfg.signer) {
		return fmt.Errorf("signer %s not owner of safe", cfg.signer.Hex())
	}

	if len(items) == 0 {
		return nil
	}

	for _, item := range items {
		data, err := ctfABI.Pack("redeemPositions", collateral, parentCollectionID, item.ConditionID, item.IndexSets)
		if err != nil {
			log.Printf("[warn] pack redeem failed condition=%s: %v", item.ConditionID.Hex(), err)
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		_, err = client.CallContract(callCtx, ethereum.CallMsg{
			From: cfg.funder,
			To:   &ctfAddr,
			Data: data,
		}, nil)
		cancel()
		if err != nil {
			log.Printf("[warn] preflight redeem failed condition=%s: %v", item.ConditionID.Hex(), err)
			continue
		}

		nonce, err := fetchSafeNonce(ctx, client, safeABI, cfg.funder)
		if err != nil {
			return err
		}

		txHash, err := safeTransactionHash(ctx, client, safeABI, cfg.funder, ctfAddr, data, nonce, 0)
		if err != nil {
			return fmt.Errorf("safe tx hash failed condition=%s: %w", item.ConditionID.Hex(), err)
		}

		signature, err := signSafeHash(txHash, cfg.privateKey)
		if err != nil {
			return fmt.Errorf("safe signature failed condition=%s: %w", item.ConditionID.Hex(), err)
		}

		tx, err := sendSafeExecTransaction(ctx, client, chainID, safeABI, cfg.privateKey, cfg.funder, ctfAddr, data, signature, 0)
		if err != nil {
			log.Printf("[warn] safe exec failed condition=%s: %v", item.ConditionID.Hex(), err)
			continue
		}
		log.Printf("[claim] sent tx=%s condition=%s nonce=%s", tx.Hash().Hex(), item.ConditionID.Hex(), nonce.String())

		receipt, err := waitForReceipt(ctx, client, tx, defaultSafeWaitTimeout)
		if err != nil {
			log.Printf("[warn] wait receipt failed condition=%s: %v", item.ConditionID.Hex(), err)
			continue
		}
		if receipt.Status != types.ReceiptStatusSuccessful {
			log.Printf("[warn] safe tx reverted condition=%s tx=%s", item.ConditionID.Hex(), tx.Hash().Hex())
			continue
		}
	}
	return nil
}

func fetchSafeMeta(ctx context.Context, client *ethclient.Client, safeABI abi.ABI, safeAddr common.Address) (int64, []common.Address, error) {
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	thresholdVals, err := callABI(callCtx, client, safeABI, safeAddr, "getThreshold")
	if err != nil {
		return 0, nil, fmt.Errorf("safe getThreshold: %w", err)
	}
	threshold := toInt64(thresholdVals, "getThreshold")
	if threshold <= 0 {
		return 0, nil, fmt.Errorf("safe threshold invalid: %d", threshold)
	}

	ownerVals, err := callABI(callCtx, client, safeABI, safeAddr, "getOwners")
	if err != nil {
		return threshold, nil, fmt.Errorf("safe getOwners: %w", err)
	}
	owners := toAddressSlice(ownerVals, "getOwners")
	if len(owners) == 0 {
		return threshold, nil, fmt.Errorf("safe owners empty")
	}

	return threshold, owners, nil
}

func fetchSafeNonce(ctx context.Context, client *ethclient.Client, safeABI abi.ABI, safeAddr common.Address) (*big.Int, error) {
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	vals, err := callABI(callCtx, client, safeABI, safeAddr, "nonce")
	if err != nil {
		return nil, fmt.Errorf("safe nonce: %w", err)
	}
	if len(vals) != 1 {
		return nil, fmt.Errorf("safe nonce: unexpected result len %d", len(vals))
	}
	n, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("safe nonce: unexpected type %T", vals[0])
	}
	return n, nil
}

func waitForReceipt(ctx context.Context, client *ethclient.Client, tx *types.Transaction, timeout time.Duration) (*types.Receipt, error) {
	waitCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return bind.WaitMined(waitCtx, client, tx)
}

func safeTransactionHash(ctx context.Context, client *ethclient.Client, safeABI abi.ABI, safeAddr, to common.Address, data []byte, nonce *big.Int, operation uint8) ([32]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	vals, err := callABI(callCtx, client, safeABI, safeAddr, "getTransactionHash",
		to,
		big.NewInt(0),
		data,
		operation,
		big.NewInt(0),
		big.NewInt(0),
		big.NewInt(0),
		common.Address{},
		common.Address{},
		nonce,
	)
	if err != nil {
		return [32]byte{}, err
	}
	if len(vals) != 1 {
		return [32]byte{}, fmt.Errorf("safe tx hash: unexpected result len %d", len(vals))
	}
	switch v := vals[0].(type) {
	case [32]byte:
		return v, nil
	case common.Hash:
		return v, nil
	default:
		return [32]byte{}, fmt.Errorf("safe tx hash: unexpected type %T", vals[0])
	}
}

func signSafeHash(hash [32]byte, pk *ecdsa.PrivateKey) ([]byte, error) {
	if pk == nil {
		return nil, errors.New("missing private key")
	}
	sig, err := crypto.Sign(hash[:], pk)
	if err != nil {
		return nil, err
	}
	if len(sig) != 65 {
		return nil, fmt.Errorf("unexpected signature length %d", len(sig))
	}
	sig[64] += 27
	return sig, nil
}

func sendSafeExecTransaction(ctx context.Context, client *ethclient.Client, chainID *big.Int, safeABI abi.ABI, pk *ecdsa.PrivateKey, safeAddr, to common.Address, data, signatures []byte, operation uint8) (*types.Transaction, error) {
	opts, err := bind.NewKeyedTransactorWithChainID(pk, chainID)
	if err != nil {
		return nil, err
	}
	opts.Context = ctx

	contract := bind.NewBoundContract(safeAddr, safeABI, client, client, client)
	return contract.Transact(opts, "execTransaction",
		to,
		big.NewInt(0),
		data,
		operation,
		big.NewInt(0),
		big.NewInt(0),
		big.NewInt(0),
		common.Address{},
		common.Address{},
		signatures,
	)
}

func callABI(ctx context.Context, client *ethclient.Client, contractABI abi.ABI, to common.Address, method string, args ...interface{}) ([]interface{}, error) {
	data, err := contractABI.Pack(method, args...)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &to, Data: data}
	out, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, err
	}
	return contractABI.Unpack(method, out)
}

func outcomeIndexToSet(idx int) (*big.Int, error) {
	if idx < 0 || idx > 255 {
		return nil, fmt.Errorf("invalid outcome index %d", idx)
	}
	return new(big.Int).Lsh(big.NewInt(1), uint(idx)), nil
}

func parseConditionID(raw string) (common.Hash, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return common.Hash{}, errors.New("empty condition id")
	}
	if !strings.HasPrefix(s, "0x") {
		return common.Hash{}, fmt.Errorf("condition id missing 0x prefix: %q", s)
	}
	hexStr := strings.TrimPrefix(s, "0x")
	if len(hexStr) != 64 {
		return common.Hash{}, fmt.Errorf("condition id length %d", len(hexStr))
	}
	if _, err := hex.DecodeString(hexStr); err != nil {
		return common.Hash{}, fmt.Errorf("condition id hex: %w", err)
	}
	return common.HexToHash(s), nil
}

func containsIndexSet(sets []*big.Int, target *big.Int) bool {
	for _, s := range sets {
		if s.Cmp(target) == 0 {
			return true
		}
	}
	return false
}

func formatIndexSets(sets []*big.Int) string {
	if len(sets) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(sets))
	for _, s := range sets {
		parts = append(parts, s.String())
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func containsAddress(addrs []common.Address, target common.Address) bool {
	for _, addr := range addrs {
		if addr == target {
			return true
		}
	}
	return false
}

func toInt64(vals []interface{}, method string) int64 {
	if len(vals) != 1 {
		log.Printf("[warn] %s: unexpected result len %d", method, len(vals))
		return 0
	}
	v, ok := vals[0].(*big.Int)
	if !ok {
		log.Printf("[warn] %s: unexpected type %T", method, vals[0])
		return 0
	}
	return v.Int64()
}

func toAddressSlice(vals []interface{}, method string) []common.Address {
	if len(vals) != 1 {
		log.Printf("[warn] %s: unexpected result len %d", method, len(vals))
		return nil
	}
	out, ok := vals[0].([]common.Address)
	if !ok {
		log.Printf("[warn] %s: unexpected type %T", method, vals[0])
		return nil
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
