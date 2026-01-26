package copytrade

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"poly-gocopy/internal/clob"
	"poly-gocopy/internal/polygonwatch"
)

type TakerTrade struct {
	TokenID string
	Side    clob.Side
	// AmountUnits is denominated in 1e6 units (same as USDC decimals).
	// For BUY: amount in collateral units (USDC micro) to spend.
	// For SELL: amount in shares units (microshares) to sell.
	AmountUnits *big.Int
}

func ComputeLeaderTakerTrade(fill *polygonwatch.FillEvent, leader common.Address, collateral common.Address) (*TakerTrade, error) {
	if fill == nil {
		return nil, fmt.Errorf("fill required")
	}
	if fill.Taker != leader {
		return nil, nil
	}
	if fill.MakerAssetId == nil || fill.TakerAssetId == nil || fill.MakerAmountFilled == nil || fill.TakerAmountFilled == nil {
		return nil, fmt.Errorf("fill missing fields")
	}

	collateralID := new(big.Int).SetBytes(collateral.Bytes())
	isCollateral := func(assetID *big.Int) bool {
		if assetID == nil {
			return false
		}
		// Some exchange deployments emit 0 as the collateral assetId.
		if assetID.Sign() == 0 {
			return true
		}
		return assetID.Cmp(collateralID) == 0
	}

	// On Polymarket exchange contracts, one side is collateral (ERC20) and one side is the outcome tokenId (uint256).
	//
	// Important: the OrderFilled ABI uses maker*/taker* to describe what each party *paid*:
	// - makerAssetId/makerAmountFilled: what the maker paid (what the taker received)
	// - takerAssetId/takerAmountFilled: what the taker paid (what the maker received)
	//
	// We copy the leader-as-taker action, so the leader "paid" side determines BUY vs SELL:
	// - If takerAssetId is collateral, the taker paid collateral => BUY tokenId (makerAssetId)
	// - If makerAssetId is collateral, the maker paid collateral => taker sold tokenId (takerAssetId)
	switch {
	case isCollateral(fill.TakerAssetId):
		// Leader (taker) paid collateral => leader bought shares.
		return &TakerTrade{
			TokenID:     fill.MakerAssetId.String(),
			Side:        clob.SideBuy,
			AmountUnits: new(big.Int).Set(fill.TakerAmountFilled),
		}, nil
	case isCollateral(fill.MakerAssetId):
		// Leader (taker) paid shares => leader sold shares for collateral.
		return &TakerTrade{
			TokenID:     fill.TakerAssetId.String(),
			Side:        clob.SideSell,
			AmountUnits: new(big.Int).Set(fill.TakerAmountFilled),
		}, nil
	default:
		// Fallback heuristic: ERC20 asset IDs are typically 160-bit addresses; tokenIds are full uint256 values.
		makerIsAddr := fill.MakerAssetId.BitLen() <= 160
		takerIsAddr := fill.TakerAssetId.BitLen() <= 160

		if takerIsAddr && !makerIsAddr {
			return &TakerTrade{
				TokenID:     fill.MakerAssetId.String(),
				Side:        clob.SideBuy,
				AmountUnits: new(big.Int).Set(fill.TakerAmountFilled),
			}, nil
		}
		if makerIsAddr && !takerIsAddr {
			return &TakerTrade{
				TokenID:     fill.TakerAssetId.String(),
				Side:        clob.SideSell,
				AmountUnits: new(big.Int).Set(fill.TakerAmountFilled),
			}, nil
		}

		return nil, fmt.Errorf("unable to classify maker/taker asset ids as collateral vs tokenId")
	}
}
