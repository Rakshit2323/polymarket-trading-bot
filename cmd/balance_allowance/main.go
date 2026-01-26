package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/dotenv"
)

func main() {
	log.SetFlags(0)

	if err := dotenv.Load(); err != nil {
		log.Printf("[warn] %v", err)
	}

	var assetTypeFlag string
	var tokenIDFlag string
	var signatureTypeFlag int
	var updateFirst bool
	var useServerTime bool
	var apiNonce uint64

	flag.StringVar(&assetTypeFlag, "asset-type", "COLLATERAL", "Asset type (COLLATERAL or CONDITIONAL); empty to omit")
	flag.StringVar(&tokenIDFlag, "token-id", "", "Conditional token id (optional)")
	flag.IntVar(&signatureTypeFlag, "signature-type", -1, "Signature type override (default: from env or client)")
	flag.BoolVar(&updateFirst, "update", true, "Call /balance-allowance/update before fetching")
	flag.BoolVar(&useServerTime, "use-server-time", true, "Use /time for signed requests")
	flag.Uint64Var(&apiNonce, "api-nonce", 0, "Nonce for API key derive/create")
	flag.Parse()

	privateKeyHex := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_PRIVATE_KEY"), os.Getenv("PRIVATE_KEY")))
	if privateKeyHex == "" {
		log.Fatalf("[fatal] private key required (set CLOB_PRIVATE_KEY/PRIVATE_KEY in .env)")
	}
	privateKeyHex = strings.TrimSpace(strings.TrimPrefix(privateKeyHex, "0x"))
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		log.Fatalf("[fatal] invalid private key: %v", err)
	}

	var funder common.Address
	if envFunder := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_FUNDER"), os.Getenv("FUNDER"))); envFunder != "" {
		if !common.IsHexAddress(envFunder) {
			log.Fatalf("[fatal] invalid FUNDER/CLOB_FUNDER %q", envFunder)
		}
		funder = common.HexToAddress(envFunder)
	}

	signatureTypeDefault := 0
	if env := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_SIGNATURE_TYPE"), os.Getenv("SIGNATURE_TYPE"))); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil {
			log.Fatalf("[fatal] invalid signature type env %q: %v", env, err)
		}
		signatureTypeDefault = v
	}
	signatureType := signatureTypeDefault
	if signatureTypeFlag >= 0 {
		signatureType = signatureTypeFlag
	}

	clobHost := strings.TrimSpace(os.Getenv("CLOB_URL"))
	if clobHost == "" {
		clobHost = "https://clob.polymarket.com"
	}

	clobClient, err := clob.NewClient(clobHost, 137, privateKey, funder, signatureType)
	if err != nil {
		log.Fatalf("[fatal] clob client: %v", err)
	}

	apiKey := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_API_KEY"), os.Getenv("API_KEY")))
	apiSecret := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_SECRET"), os.Getenv("SECRET")))
	apiPass := strings.TrimSpace(firstNonEmpty(os.Getenv("CLOB_PASSPHRASE"), os.Getenv("PASSPHRASE")))
	if apiKey != "" && apiSecret != "" && apiPass != "" {
		clobClient.SetApiCreds(clob.ApiKeyCreds{Key: apiKey, Secret: apiSecret, Passphrase: apiPass})
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		creds, err := clobClient.CreateOrDeriveApiKey(ctx, apiNonce, useServerTime)
		if err != nil {
			log.Fatalf("[fatal] failed to create/derive api key: %v", err)
		}
		clobClient.SetApiCreds(creds)
	}

	params := &clob.BalanceAllowanceParams{
		AssetType:     strings.TrimSpace(assetTypeFlag),
		TokenID:       strings.TrimSpace(tokenIDFlag),
		SignatureType: signatureTypeFlag,
	}

	if updateFirst {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		resp, err := clobClient.UpdateBalanceAllowance(ctx, params, useServerTime)
		if err != nil {
			log.Fatalf("[fatal] update balance/allowance: %v", err)
		}
		printJSON("update", resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	resp, err := clobClient.GetBalanceAllowance(ctx, params, useServerTime)
	if err != nil {
		log.Fatalf("[fatal] get balance/allowance: %v", err)
	}
	printJSON("get", resp)
}

func printJSON(label string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatalf("[fatal] %s marshal: %v", label, err)
	}
	fmt.Printf("[%s]\n%s\n", label, string(b))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
