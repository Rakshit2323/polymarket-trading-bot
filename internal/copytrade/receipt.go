package copytrade

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"poly-gocopy/internal/clob"
)

type ReceiptTrade struct {
	TokenID string
	Side    clob.Side

	// AmountUnits matches the semantics of TakerTrade:
	// - BUY: collateral (USDC micro) to spend
	// - SELL: shares (microshares) to sell
	AmountUnits *big.Int

	// Exact (unscaled) amounts derived from receipt transfers between leader and exchange.
	// - BUY: maker=collateral, taker=shares
	// - SELL: maker=shares, taker=collateral
	MakerAmountUnits *big.Int
	TakerAmountUnits *big.Int
}

var (
	erc20TransferTopic          = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	erc1155TransferSingleTopic  = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))
	erc1155TransferBatchTopic   = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))
	errLeaderTradeNotFound      = fmt.Errorf("leader trade not found in receipt transfers")
	errLeaderTradeAmbiguous     = fmt.Errorf("leader trade ambiguous (multiple token ids)")
	errLeaderTradeInconsistent  = fmt.Errorf("leader trade inconsistent (token/usdc deltas not aligned)")
)

// InferLeaderTradeFromReceipt derives the leader's effective trade by looking at actual token movements
// between the leader address and the Polymarket exchange contracts within the transaction receipt.
//
// This is intentionally receipt-driven (not OrderFilled-driven) because some fills settle by minting/splitting
// conditional tokens, and the OrderFilled maker/taker asset ids do not necessarily match what the leader
// actually sent/received on-chain.
func InferLeaderTradeFromReceipt(
	receipt *types.Receipt,
	leader common.Address,
	exchangeAddrs []common.Address,
	collateral common.Address,
) (*ReceiptTrade, error) {
	if receipt == nil {
		return nil, fmt.Errorf("receipt required")
	}
	if len(exchangeAddrs) == 0 {
		return nil, fmt.Errorf("exchange addresses required")
	}

	exSet := make(map[common.Address]struct{}, len(exchangeAddrs))
	for _, a := range exchangeAddrs {
		exSet[a] = struct{}{}
	}

	usdcDelta := new(big.Int) // positive = leader received USDC; negative = leader paid USDC
	tokenDeltas := make(map[string]*big.Int)

	addTokenDelta := func(tokenID *big.Int, delta *big.Int) {
		if tokenID == nil || delta == nil || delta.Sign() == 0 {
			return
		}
		id := tokenID.String()
		if tokenDeltas[id] == nil {
			tokenDeltas[id] = new(big.Int)
		}
		tokenDeltas[id].Add(tokenDeltas[id], delta)
	}

	for _, lg := range receipt.Logs {
		if lg == nil || len(lg.Topics) == 0 {
			continue
		}

		switch lg.Topics[0] {
		case erc20TransferTopic:
			// Only consider collateral transfers.
			if lg.Address != collateral {
				continue
			}
			if len(lg.Topics) < 3 || len(lg.Data) < 32 {
				continue
			}
			from := common.BytesToAddress(lg.Topics[1].Bytes())
			to := common.BytesToAddress(lg.Topics[2].Bytes())
			value := new(big.Int).SetBytes(lg.Data[:32])
			if value.Sign() <= 0 {
				continue
			}

			_, fromIsEx := exSet[from]
			_, toIsEx := exSet[to]

			switch {
			case from == leader && toIsEx:
				usdcDelta.Sub(usdcDelta, value)
			case fromIsEx && to == leader:
				usdcDelta.Add(usdcDelta, value)
			default:
				continue
			}

		case erc1155TransferSingleTopic:
			if len(lg.Topics) < 4 || len(lg.Data) < 64 {
				continue
			}
			operator := common.BytesToAddress(lg.Topics[1].Bytes())
			from := common.BytesToAddress(lg.Topics[2].Bytes())
			to := common.BytesToAddress(lg.Topics[3].Bytes())
			if _, ok := exSet[operator]; !ok {
				continue
			}

			id := new(big.Int).SetBytes(lg.Data[:32])
			value := new(big.Int).SetBytes(lg.Data[32:64])
			if value.Sign() <= 0 {
				continue
			}

			_, fromIsEx := exSet[from]
			_, toIsEx := exSet[to]

			switch {
			case fromIsEx && to == leader:
				addTokenDelta(id, value)
			case from == leader && toIsEx:
				addTokenDelta(id, new(big.Int).Neg(value))
			default:
				continue
			}

		case erc1155TransferBatchTopic:
			if len(lg.Topics) < 4 || len(lg.Data) < 64 {
				continue
			}
			operator := common.BytesToAddress(lg.Topics[1].Bytes())
			from := common.BytesToAddress(lg.Topics[2].Bytes())
			to := common.BytesToAddress(lg.Topics[3].Bytes())
			if _, ok := exSet[operator]; !ok {
				continue
			}

			ids, values, err := decodeERC1155TransferBatchData(lg.Data)
			if err != nil {
				continue
			}
			if len(ids) != len(values) {
				continue
			}

			_, fromIsEx := exSet[from]
			_, toIsEx := exSet[to]

			for i := range ids {
				id := ids[i]
				val := values[i]
				if id == nil || val == nil || val.Sign() <= 0 {
					continue
				}
				switch {
				case fromIsEx && to == leader:
					addTokenDelta(id, val)
				case from == leader && toIsEx:
					addTokenDelta(id, new(big.Int).Neg(val))
				default:
					continue
				}
			}
		}
	}

	nonZeroTokenIDs := make([]string, 0, len(tokenDeltas))
	for id, delta := range tokenDeltas {
		if delta != nil && delta.Sign() != 0 {
			nonZeroTokenIDs = append(nonZeroTokenIDs, id)
		}
	}

	if len(nonZeroTokenIDs) == 0 || usdcDelta.Sign() == 0 {
		return nil, errLeaderTradeNotFound
	}
	if len(nonZeroTokenIDs) != 1 {
		return nil, fmt.Errorf("%w: tokenIDs=%v usdcDelta=%s", errLeaderTradeAmbiguous, nonZeroTokenIDs, usdcDelta.String())
	}

	tokenID := nonZeroTokenIDs[0]
	tokenDelta := tokenDeltas[tokenID]
	if tokenDelta == nil || tokenDelta.Sign() == 0 {
		return nil, errLeaderTradeNotFound
	}

	switch {
	case tokenDelta.Sign() > 0 && usdcDelta.Sign() < 0:
		shares := new(big.Int).Set(tokenDelta)
		collateralSpent := new(big.Int).Neg(usdcDelta)
		return &ReceiptTrade{
			TokenID:          tokenID,
			Side:            clob.SideBuy,
			AmountUnits:      new(big.Int).Set(collateralSpent),
			MakerAmountUnits: new(big.Int).Set(collateralSpent),
			TakerAmountUnits: new(big.Int).Set(shares),
		}, nil
	case tokenDelta.Sign() < 0 && usdcDelta.Sign() > 0:
		sharesSold := new(big.Int).Neg(tokenDelta)
		collateralRecv := new(big.Int).Set(usdcDelta)
		return &ReceiptTrade{
			TokenID:          tokenID,
			Side:            clob.SideSell,
			AmountUnits:      new(big.Int).Set(sharesSold),
			MakerAmountUnits: new(big.Int).Set(sharesSold),
			TakerAmountUnits: new(big.Int).Set(collateralRecv),
		}, nil
	default:
		return nil, fmt.Errorf("%w: tokenDelta=%s usdcDelta=%s", errLeaderTradeInconsistent, tokenDelta.String(), usdcDelta.String())
	}
}

func decodeERC1155TransferBatchData(data []byte) ([]*big.Int, []*big.Int, error) {
	// ABI encoding:
	// data = offset(ids) | offset(values) | ...dynamic...
	if len(data) < 64 {
		return nil, nil, fmt.Errorf("batch data too short")
	}
	idsOffset := int(new(big.Int).SetBytes(data[:32]).Int64())
	valsOffset := int(new(big.Int).SetBytes(data[32:64]).Int64())

	ids, err := decodeU256Slice(data, idsOffset)
	if err != nil {
		return nil, nil, err
	}
	vals, err := decodeU256Slice(data, valsOffset)
	if err != nil {
		return nil, nil, err
	}
	return ids, vals, nil
}

func decodeU256Slice(data []byte, offset int) ([]*big.Int, error) {
	if offset < 0 || offset+32 > len(data) {
		return nil, fmt.Errorf("invalid offset %d", offset)
	}
	n := int(new(big.Int).SetBytes(data[offset : offset+32]).Int64())
	if n < 0 {
		return nil, fmt.Errorf("negative length")
	}
	start := offset + 32
	end := start + n*32
	if end > len(data) {
		return nil, fmt.Errorf("slice out of range")
	}

	out := make([]*big.Int, 0, n)
	for i := 0; i < n; i++ {
		w := start + i*32
		out = append(out, new(big.Int).SetBytes(data[w:w+32]))
	}
	return out, nil
}

