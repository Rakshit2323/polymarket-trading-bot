package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"poly-gocopy/internal/dotenv"
	"poly-gocopy/internal/polygonutil"
)

const microsScale = uint64(1_000_000)

func main() {
	log.SetFlags(0)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	var addrFlag string
	flag.StringVar(&addrFlag, "address", "", "Wallet address to check (default: FUNDER/CLOB_FUNDER or signer from PRIVATE_KEY)")
	flag.Parse()

	rpcURL, err := polygonutil.RPCURLFromEnv()
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	owner, ownerSrc, err := resolveOwnerAddress(addrFlag)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	usdcMicros, err := polygonutil.USDCTokenBalanceMicros(ctx, rpcURL, owner)
	if err != nil {
		log.Fatalf("[fatal] %v", err)
	}

	half := usdcMicros / 2

	fmt.Printf("owner: %s (%s)\n", owner.Hex(), ownerSrc)
	fmt.Printf("usdc_balance: %s (micros=%d)\n", formatMicros(usdcMicros), usdcMicros)
	fmt.Printf("half_split: %s (cap_a=%s cap_b=%s)\n", formatMicros(half), formatMicros(half), formatMicros(half))
}

func resolveOwnerAddress(addrFlag string) (common.Address, string, error) {
	if strings.TrimSpace(addrFlag) != "" {
		raw := strings.TrimSpace(addrFlag)
		if !common.IsHexAddress(raw) {
			return common.Address{}, "", fmt.Errorf("invalid --address %q", raw)
		}
		return common.HexToAddress(raw), "--address", nil
	}

	if envFunder := firstNonEmpty(os.Getenv("CLOB_FUNDER"), os.Getenv("FUNDER")); strings.TrimSpace(envFunder) != "" {
		if !common.IsHexAddress(envFunder) {
			return common.Address{}, "", fmt.Errorf("invalid FUNDER/CLOB_FUNDER env %q", envFunder)
		}
		return common.HexToAddress(envFunder), "FUNDER", nil
	}

	if pkHex := firstNonEmpty(os.Getenv("CLOB_PRIVATE_KEY"), os.Getenv("PRIVATE_KEY")); strings.TrimSpace(pkHex) != "" {
		pkHex = strings.TrimSpace(strings.TrimPrefix(pkHex, "0x"))
		pk, err := crypto.HexToECDSA(pkHex)
		if err != nil {
			return common.Address{}, "", fmt.Errorf("invalid PRIVATE_KEY: %w", err)
		}
		return crypto.PubkeyToAddress(pk.PublicKey), "PRIVATE_KEY", nil
	}

	return common.Address{}, "", fmt.Errorf("wallet required: set FUNDER/CLOB_FUNDER, PRIVATE_KEY/CLOB_PRIVATE_KEY, or pass --address")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
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
