package polygonwatch

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type FillEvent struct {
	TxHash       common.Hash
	BlockHash    common.Hash
	BlockNumber  uint64
	LogIndex     uint
	Removed      bool
	ReceivedAtMs int64

	OrderHash common.Hash
	Maker     common.Address
	Taker     common.Address

	MakerAssetId      *big.Int
	TakerAssetId      *big.Int
	MakerAmountFilled *big.Int
	TakerAmountFilled *big.Int
	Fee               *big.Int
}

func DecodeOrderFilledLog(vLog types.Log) (*FillEvent, error) {
	// topics:
	// 0: event sig
	// 1: orderHash (bytes32 indexed)
	// 2: maker (address indexed)
	// 3: taker (address indexed)
	if len(vLog.Topics) < 4 {
		return nil, fmt.Errorf("unexpected topics len=%d", len(vLog.Topics))
	}
	if len(vLog.Data) < 32*5 {
		return nil, fmt.Errorf("unexpected data len=%d", len(vLog.Data))
	}

	readU256 := func(word int) *big.Int {
		start := word * 32
		end := start + 32
		return new(big.Int).SetBytes(vLog.Data[start:end])
	}

	return &FillEvent{
		TxHash:      vLog.TxHash,
		BlockHash:   vLog.BlockHash,
		BlockNumber: vLog.BlockNumber,
		LogIndex:    vLog.Index,
		Removed:     vLog.Removed,

		OrderHash: vLog.Topics[1],
		Maker:     common.BytesToAddress(vLog.Topics[2].Bytes()),
		Taker:     common.BytesToAddress(vLog.Topics[3].Bytes()),

		MakerAssetId:      readU256(0),
		TakerAssetId:      readU256(1),
		MakerAmountFilled: readU256(2),
		TakerAmountFilled: readU256(3),
		Fee:               readU256(4),
	}, nil
}
