package ethutil

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestParseAddressList(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		got, err := ParseAddressList("   \n\t")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil, got %#v", got)
		}
	})

	t.Run("single", func(t *testing.T) {
		got, err := ParseAddressList("0x0000000000000000000000000000000000000001")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(got) != 1 || got[0] != common.HexToAddress("0x1") {
			t.Fatalf("unexpected result: %#v", got)
		}
	})

	t.Run("csv+whitespace+dedupe", func(t *testing.T) {
		got, err := ParseAddressList("0x0000000000000000000000000000000000000001, 0x0000000000000000000000000000000000000002\n0x0000000000000000000000000000000000000001")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2, got %d: %#v", len(got), got)
		}
		if got[0] != common.HexToAddress("0x1") || got[1] != common.HexToAddress("0x2") {
			t.Fatalf("unexpected order: %#v", got)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseAddressList("0xnotanaddress")
		if err == nil {
			t.Fatalf("expected err")
		}
	})
}
