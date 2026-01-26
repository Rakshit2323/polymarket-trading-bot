package copytrade

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestInferLeaderTradeFromReceipt_BuyLikeTx5400(t *testing.T) {
	// Mirrors the observed transfer pattern for tx 0x5400923c...:
	// - Leader pays 1 USDC to exchange
	// - Leader receives 10 shares (tokenId=701736...) from exchange
	leader := common.HexToAddress("0x49226C9a8eae5b040f4aa878369C6ab130985B4C")
	exchange := common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
	usdc := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

	tokenID := new(big.Int)
	tokenID.SetString("70173651533867133813037778810002894047944029658657013054789389758024394038176", 10)

	erc20Transfer := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	erc1155TransferSingle := crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))

	usdcOut := &types.Log{
		Address: usdc,
		Topics: []common.Hash{
			erc20Transfer,
			common.BytesToHash(leader.Bytes()),
			common.BytesToHash(exchange.Bytes()),
		},
		Data: new(big.Int).SetInt64(1_000_000).FillBytes(make([]byte, 32)),
	}

	var erc1155Data [64]byte
	copy(erc1155Data[:32], tokenID.FillBytes(make([]byte, 32)))
	copy(erc1155Data[32:64], new(big.Int).SetInt64(10_000_000).FillBytes(make([]byte, 32)))

	sharesIn := &types.Log{
		Address: common.HexToAddress("0x4d97dcd97ec945f40cf65f87097ace5ea0476045"), // conditional tokens (not strictly required)
		Topics: []common.Hash{
			erc1155TransferSingle,
			common.BytesToHash(exchange.Bytes()), // operator
			common.BytesToHash(exchange.Bytes()), // from
			common.BytesToHash(leader.Bytes()),   // to
		},
		Data: erc1155Data[:],
	}

	receipt := &types.Receipt{Logs: []*types.Log{usdcOut, sharesIn}}
	trade, err := InferLeaderTradeFromReceipt(receipt, leader, []common.Address{exchange}, usdc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade.Side != "BUY" {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != tokenID.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, tokenID.String())
	}
	if got, want := trade.MakerAmountUnits.String(), "1000000"; got != want {
		t.Fatalf("maker amount mismatch: got %s want %s", got, want)
	}
	if got, want := trade.TakerAmountUnits.String(), "10000000"; got != want {
		t.Fatalf("taker amount mismatch: got %s want %s", got, want)
	}
	if got, want := trade.AmountUnits.String(), "1000000"; got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}

func TestInferLeaderTradeFromReceipt_SellLikeTxB5A8(t *testing.T) {
	// Mirrors the observed transfer pattern for tx 0xb5a8a464...:
	// - Leader sends 20 shares (tokenId=701736...) to exchange
	// - Leader receives 1.6 USDC from exchange
	leader := common.HexToAddress("0x49226C9a8eae5b040f4aa878369C6ab130985B4C")
	exchange := common.HexToAddress("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
	usdc := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

	tokenID := new(big.Int)
	tokenID.SetString("70173651533867133813037778810002894047944029658657013054789389758024394038176", 10)

	erc20Transfer := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	erc1155TransferSingle := crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))

	var erc1155Data [64]byte
	copy(erc1155Data[:32], tokenID.FillBytes(make([]byte, 32)))
	copy(erc1155Data[32:64], new(big.Int).SetInt64(20_000_000).FillBytes(make([]byte, 32)))

	sharesOut := &types.Log{
		Address: common.HexToAddress("0x4d97dcd97ec945f40cf65f87097ace5ea0476045"),
		Topics: []common.Hash{
			erc1155TransferSingle,
			common.BytesToHash(exchange.Bytes()), // operator
			common.BytesToHash(leader.Bytes()),   // from
			common.BytesToHash(exchange.Bytes()), // to
		},
		Data: erc1155Data[:],
	}

	usdcIn := &types.Log{
		Address: usdc,
		Topics: []common.Hash{
			erc20Transfer,
			common.BytesToHash(exchange.Bytes()),
			common.BytesToHash(leader.Bytes()),
		},
		Data: new(big.Int).SetInt64(1_600_000).FillBytes(make([]byte, 32)),
	}

	receipt := &types.Receipt{Logs: []*types.Log{sharesOut, usdcIn}}
	trade, err := InferLeaderTradeFromReceipt(receipt, leader, []common.Address{exchange}, usdc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade.Side != "SELL" {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != tokenID.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, tokenID.String())
	}
	if got, want := trade.MakerAmountUnits.String(), "20000000"; got != want {
		t.Fatalf("maker amount mismatch: got %s want %s", got, want)
	}
	if got, want := trade.TakerAmountUnits.String(), "1600000"; got != want {
		t.Fatalf("taker amount mismatch: got %s want %s", got, want)
	}
	if got, want := trade.AmountUnits.String(), "20000000"; got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}

