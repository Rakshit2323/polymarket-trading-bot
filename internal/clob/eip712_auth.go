package clob

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const clobAuthMessage = "This message attests that I control the given wallet"

var (
	clobAuthDomainNameHash    = crypto.Keccak256Hash([]byte("ClobAuthDomain"))
	clobAuthDomainVersionHash = crypto.Keccak256Hash([]byte("1"))

	clobAuthTypeHash = crypto.Keccak256Hash([]byte("ClobAuth(address address,string timestamp,uint256 nonce,string message)"))

	bytes32Ty    = mustABIType("bytes32")
	addressTy    = mustABIType("address")
	uint256Ty    = mustABIType("uint256")
	stringHashTy = bytes32Ty
)

func mustABIType(t string) abi.Type {
	ty, err := abi.NewType(t, "", nil)
	if err != nil {
		panic(err)
	}
	return ty
}

func buildClobAuthDomainSeparator(chainID int64) (common.Hash, error) {
	chain := big.NewInt(chainID)

	types := []abi.Type{bytes32Ty, bytes32Ty, bytes32Ty, uint256Ty}
	values := []any{
		crypto.Keccak256Hash([]byte("EIP712Domain(string name,string version,uint256 chainId)")),
		clobAuthDomainNameHash,
		clobAuthDomainVersionHash,
		chain,
	}
	encoded, err := abi.Arguments{
		{Type: types[0]},
		{Type: types[1]},
		{Type: types[2]},
		{Type: types[3]},
	}.Pack(values...)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(encoded), nil
}

func buildClobEip712Signature(privateKey *ecdsa.PrivateKey, signer common.Address, chainID int64, timestamp int64, nonce uint64) (string, error) {
	domainSeparator, err := buildClobAuthDomainSeparator(chainID)
	if err != nil {
		return "", err
	}

	tsStr := fmt.Sprintf("%d", timestamp)
	tsHash := crypto.Keccak256Hash([]byte(tsStr))
	msgHash := crypto.Keccak256Hash([]byte(clobAuthMessage))

	values := []any{
		clobAuthTypeHash,
		signer,
		tsHash, // EIP712 encodes dynamic types as keccak256(value)
		new(big.Int).SetUint64(nonce),
		msgHash, // keccak256(message)
	}
	args := abi.Arguments{
		{Type: bytes32Ty},
		{Type: addressTy},
		{Type: stringHashTy},
		{Type: uint256Ty},
		{Type: stringHashTy},
	}
	encoded, err := args.Pack(values...)
	if err != nil {
		return "", err
	}

	structHash := crypto.Keccak256Hash(encoded)
	raw := make([]byte, 0, 2+32+32)
	raw = append(raw, 0x19, 0x01)
	raw = append(raw, domainSeparator.Bytes()...)
	raw = append(raw, structHash.Bytes()...)
	digest := crypto.Keccak256Hash(raw)

	sig, err := crypto.Sign(digest.Bytes(), privateKey)
	if err != nil {
		return "", err
	}
	sig[64] += 27
	return "0x" + common.Bytes2Hex(sig), nil
}
