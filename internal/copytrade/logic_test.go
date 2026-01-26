package copytrade

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/polygonwatch"
)

func TestComputeLeaderTakerTrade_TakerPaysCollateralIsBuy(t *testing.T) {
	leader := common.HexToAddress("0x1111111111111111111111111111111111111111")
	collateral := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
	collateralID := new(big.Int).SetBytes(collateral.Bytes())

	fill := &polygonwatch.FillEvent{
		Maker: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Taker: leader,
		// taker* fields are what taker paid (collateral)
		MakerAssetId:      big.NewInt(123456),
		MakerAmountFilled: big.NewInt(55_000_000), // shares in 1e6 units (example)
		TakerAssetId:      collateralID,
		TakerAmountFilled: big.NewInt(50_000_000), // $50 in 1e6 units
	}

	trade, err := ComputeLeaderTakerTrade(fill, leader, collateral)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade == nil {
		t.Fatalf("expected trade")
	}
	if trade.Side != clob.SideBuy {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != fill.MakerAssetId.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, fill.MakerAssetId.String())
	}
	if got, want := trade.AmountUnits.String(), fill.TakerAmountFilled.String(); got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}

func TestComputeLeaderTakerTrade_TakerPaysSharesIsSell(t *testing.T) {
	leader := common.HexToAddress("0x1111111111111111111111111111111111111111")
	collateral := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
	collateralID := new(big.Int).SetBytes(collateral.Bytes())

	fill := &polygonwatch.FillEvent{
		Maker: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Taker: leader,
		// maker paid collateral, taker paid shares
		MakerAssetId:      collateralID,
		MakerAmountFilled: big.NewInt(9_000_000), // $9 in 1e6 units
		TakerAssetId:      big.NewInt(999999),
		TakerAmountFilled: big.NewInt(10_000_000), // 10 shares in 1e6 units
	}

	trade, err := ComputeLeaderTakerTrade(fill, leader, collateral)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade == nil {
		t.Fatalf("expected trade")
	}
	if trade.Side != clob.SideSell {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != fill.TakerAssetId.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, fill.TakerAssetId.String())
	}
	if got, want := trade.AmountUnits.String(), fill.TakerAmountFilled.String(); got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}

func TestComputeLeaderTakerTrade_CollateralZeroSentinelIsBuy(t *testing.T) {
	leader := common.HexToAddress("0x1111111111111111111111111111111111111111")
	collateral := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

	tokenID := new(big.Int)
	tokenID.SetString("43577760886052680570334039145361508464602899119356427453668205933543171672461", 10)

	fill := &polygonwatch.FillEvent{
		Maker:             common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Taker:             leader,
		MakerAssetId:      tokenID,
		MakerAmountFilled: big.NewInt(10_000_000),
		TakerAssetId:      big.NewInt(0), // collateral sentinel
		TakerAmountFilled: big.NewInt(9_000_000),
	}

	trade, err := ComputeLeaderTakerTrade(fill, leader, collateral)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade == nil {
		t.Fatalf("expected trade")
	}
	if trade.Side != clob.SideBuy {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != tokenID.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, tokenID.String())
	}
	if got, want := trade.AmountUnits.String(), "9000000"; got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}

func TestComputeLeaderTakerTrade_CollateralZeroSentinelIsSell(t *testing.T) {
	leader := common.HexToAddress("0x1111111111111111111111111111111111111111")
	collateral := common.HexToAddress("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

	tokenID := new(big.Int)
	tokenID.SetString("43577760886052680570334039145361508464602899119356427453668205933543171672461", 10)

	fill := &polygonwatch.FillEvent{
		Maker:             common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Taker:             leader,
		MakerAssetId:      big.NewInt(0), // collateral sentinel
		MakerAmountFilled: big.NewInt(9_000_000),
		TakerAssetId:      tokenID,
		TakerAmountFilled: big.NewInt(10_000_000),
	}

	trade, err := ComputeLeaderTakerTrade(fill, leader, collateral)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade == nil {
		t.Fatalf("expected trade")
	}
	if trade.Side != clob.SideSell {
		t.Fatalf("side mismatch: got %s", trade.Side)
	}
	if trade.TokenID != tokenID.String() {
		t.Fatalf("token mismatch: got %s want %s", trade.TokenID, tokenID.String())
	}
	if got, want := trade.AmountUnits.String(), "10000000"; got != want {
		t.Fatalf("amount mismatch: got %s want %s", got, want)
	}
}
