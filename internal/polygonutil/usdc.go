package polygonutil

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

const USDCTokenDecimals = 6

var USDCTokenAddress = common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

var erc20BalanceOfSelector = crypto.Keccak256([]byte("balanceOf(address)"))[:4]
var erc20AllowanceSelector = crypto.Keccak256([]byte("allowance(address,address)"))[:4]

func uint64FromUint256Saturating(x *big.Int) uint64 {
	// USDC amounts (6 decimals) easily fit in uint64 for balances and per-trade
	// accounting, but allowances are frequently set to max(uint256) which does not.
	if x == nil {
		return 0
	}
	if x.Sign() <= 0 {
		return 0
	}
	if x.IsUint64() {
		return x.Uint64()
	}
	return math.MaxUint64
}

func RPCURLFromEnv() (string, error) {
	rpcURL := strings.TrimSpace(firstNonEmpty(os.Getenv("RPC_WS_URL"), os.Getenv("RPC_URL"), os.Getenv("POLYGON_WS_URL")))
	if rpcURL == "" {
		return "", fmt.Errorf("RPC_WS_URL or RPC_URL required (set RPC_WS_URL in .env)")
	}
	if !strings.HasPrefix(rpcURL, "wss") && !strings.HasPrefix(rpcURL, "http") {
		return "", fmt.Errorf("polygon RPC URL must be wss://... or http(s)://..., got %q", rpcURL)
	}
	if strings.Contains(rpcURL, "YOUR_KEY") {
		return "", fmt.Errorf("polygon RPC URL still contains placeholder YOUR_KEY. Set RPC_WS_URL/RPC_URL to your provider URL")
	}
	return rpcURL, nil
}

func USDCTokenBalanceMicros(ctx context.Context, rpcURL string, owner common.Address) (uint64, error) {
	if strings.TrimSpace(rpcURL) == "" {
		return 0, fmt.Errorf("polygon RPC URL missing")
	}
	if (owner == common.Address{}) {
		return 0, fmt.Errorf("owner address missing")
	}

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return 0, fmt.Errorf("dial polygon RPC: %w", err)
	}
	defer client.Close()

	data := make([]byte, 0, 4+32)
	data = append(data, erc20BalanceOfSelector...)
	data = append(data, common.LeftPadBytes(owner.Bytes(), 32)...)

	out, err := client.CallContract(ctx, ethereum.CallMsg{To: &USDCTokenAddress, Data: data}, nil)
	if err != nil {
		return 0, fmt.Errorf("usdc balanceOf(%s): %w", owner.Hex(), err)
	}
	if len(out) == 0 {
		return 0, fmt.Errorf("usdc balanceOf returned empty result")
	}

	bal := new(big.Int).SetBytes(out)
	if !bal.IsUint64() {
		return 0, fmt.Errorf("usdc balance overflows uint64")
	}
	return bal.Uint64(), nil
}

func USDCTokenBalanceAndAllowancesMicros(ctx context.Context, rpcURL string, owner common.Address, spenders []common.Address) (balanceMicros uint64, allowances map[common.Address]uint64, err error) {
	if strings.TrimSpace(rpcURL) == "" {
		return 0, nil, fmt.Errorf("polygon RPC URL missing")
	}
	if (owner == common.Address{}) {
		return 0, nil, fmt.Errorf("owner address missing")
	}

	uniqueSpenders := make([]common.Address, 0, len(spenders))
	seen := make(map[common.Address]struct{}, len(spenders))
	for _, sp := range spenders {
		if (sp == common.Address{}) {
			continue
		}
		if _, ok := seen[sp]; ok {
			continue
		}
		seen[sp] = struct{}{}
		uniqueSpenders = append(uniqueSpenders, sp)
	}

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return 0, nil, fmt.Errorf("dial polygon RPC: %w", err)
	}
	defer client.Close()

	callUint256 := func(to common.Address, data []byte) (*big.Int, error) {
		out, err := client.CallContract(ctx, ethereum.CallMsg{To: &to, Data: data}, nil)
		if err != nil {
			return nil, err
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("empty result")
		}
		return new(big.Int).SetBytes(out), nil
	}

	// balanceOf(owner)
	balData := make([]byte, 0, 4+32)
	balData = append(balData, erc20BalanceOfSelector...)
	balData = append(balData, common.LeftPadBytes(owner.Bytes(), 32)...)
	bal, err := callUint256(USDCTokenAddress, balData)
	if err != nil {
		return 0, nil, fmt.Errorf("usdc balanceOf(%s): %w", owner.Hex(), err)
	}
	if !bal.IsUint64() {
		return 0, nil, fmt.Errorf("usdc balance overflows uint64")
	}

	allowances = make(map[common.Address]uint64, len(uniqueSpenders))
	for _, sp := range uniqueSpenders {
		// allowance(owner, spender)
		data := make([]byte, 0, 4+32+32)
		data = append(data, erc20AllowanceSelector...)
		data = append(data, common.LeftPadBytes(owner.Bytes(), 32)...)
		data = append(data, common.LeftPadBytes(sp.Bytes(), 32)...)
		a, err := callUint256(USDCTokenAddress, data)
		if err != nil {
			return 0, nil, fmt.Errorf("usdc allowance(%s,%s): %w", owner.Hex(), sp.Hex(), err)
		}
		// Allowances are commonly max(uint256). Saturate to MaxUint64 so diagnostics
		// and safety checks don't fail when allowance is effectively "unlimited".
		allowances[sp] = uint64FromUint256Saturating(a)
	}

	return bal.Uint64(), allowances, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
