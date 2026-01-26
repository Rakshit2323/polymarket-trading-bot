package ethutil

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// ParseAddressList parses a list of hex addresses from a single string.
//
// Supported separators: commas and whitespace (space/newline/tab), plus semicolons.
// Duplicate addresses are ignored (first occurrence wins).
//
// Returns (nil, nil) if raw is empty/whitespace.
func ParseAddressList(raw string) ([]common.Address, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		switch r {
		case ',', ';', ' ', '\n', '\r', '\t':
			return true
		default:
			return false
		}
	})

	out := make([]common.Address, 0, len(parts))
	seen := make(map[common.Address]struct{}, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		if !common.IsHexAddress(s) {
			return nil, fmt.Errorf("invalid hex address %q in %q", s, raw)
		}

		addr := common.HexToAddress(s)
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no addresses found in %q", raw)
	}
	return out, nil
}

func AddressSet(addrs []common.Address) map[common.Address]struct{} {
	out := make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		out[a] = struct{}{}
	}
	return out
}

func SortedAddresses(addrs []common.Address) []common.Address {
	if len(addrs) <= 1 {
		return append([]common.Address(nil), addrs...)
	}
	out := append([]common.Address(nil), addrs...)
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Bytes(), out[j].Bytes()) < 0
	})
	return out
}

func AddressesToTopics(addrs []common.Address) []common.Hash {
	out := make([]common.Hash, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, common.BytesToHash(a.Bytes()))
	}
	return out
}

func JoinHex(addrs []common.Address) string {
	if len(addrs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(addrs))
	for _, a := range addrs {
		parts = append(parts, a.Hex())
	}
	return strings.Join(parts, ",")
}
