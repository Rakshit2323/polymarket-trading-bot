package polygonwatch

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestDecodeOrderFilledLog(t *testing.T) {
	var data [32 * 5]byte
	put := func(word int, v uint64) {
		b := new(big.Int).SetUint64(v).FillBytes(make([]byte, 32))
		copy(data[word*32:(word+1)*32], b)
	}
	put(0, 11)
	put(1, 22)
	put(2, 33)
	put(3, 44)
	put(4, 55)

	vLog := types.Log{
		TxHash:      common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		BlockHash:   common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		BlockNumber: 123,
		Index:       7,
		Topics: []common.Hash{
			common.HexToHash("0x01"),
			common.HexToHash("0x02"),
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
			common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
		},
		Data: data[:],
	}

	fill, err := DecodeOrderFilledLog(vLog)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fill.MakerAssetId.String() != "11" || fill.TakerAssetId.String() != "22" {
		t.Fatalf("assetId decode mismatch: maker=%s taker=%s", fill.MakerAssetId, fill.TakerAssetId)
	}
	if fill.MakerAmountFilled.String() != "33" || fill.TakerAmountFilled.String() != "44" || fill.Fee.String() != "55" {
		t.Fatalf("amount decode mismatch: makerAmt=%s takerAmt=%s fee=%s", fill.MakerAmountFilled, fill.TakerAmountFilled, fill.Fee)
	}
	if fill.LogIndex != 7 || fill.BlockNumber != 123 {
		t.Fatalf("cursor mismatch: block=%d idx=%d", fill.BlockNumber, fill.LogIndex)
	}
}
