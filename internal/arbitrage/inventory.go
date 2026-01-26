package arbitrage

import (
	"sort"
)

type Lot struct {
	PriceMicros  uint64
	SharesMicros uint64
}

type Inventory struct {
	// lots is sorted by PriceMicros ascending.
	lots []Lot
}

func (inv *Inventory) AddLot(priceMicros, sharesMicros uint64) {
	if inv == nil || sharesMicros == 0 {
		return
	}
	i := sort.Search(len(inv.lots), func(i int) bool {
		return inv.lots[i].PriceMicros >= priceMicros
	})
	if i < len(inv.lots) && inv.lots[i].PriceMicros == priceMicros {
		inv.lots[i].SharesMicros += sharesMicros
		return
	}
	inv.lots = append(inv.lots, Lot{})
	copy(inv.lots[i+1:], inv.lots[i:])
	inv.lots[i] = Lot{PriceMicros: priceMicros, SharesMicros: sharesMicros}
}

func (inv *Inventory) CheapestPriceMicros() (uint64, bool) {
	if inv == nil || len(inv.lots) == 0 {
		return 0, false
	}
	return inv.lots[0].PriceMicros, true
}

func (inv *Inventory) TotalSharesMicros() uint64 {
	if inv == nil {
		return 0
	}
	var sum uint64
	for i := range inv.lots {
		sum += inv.lots[i].SharesMicros
	}
	return sum
}

func (inv *Inventory) TotalCostMicros() uint64 {
	if inv == nil {
		return 0
	}
	var sum uint64
	for i := range inv.lots {
		sum += costMicrosForShares(inv.lots[i].SharesMicros, inv.lots[i].PriceMicros)
	}
	return sum
}

// RemoveSharesMicros removes up to sharesMicros from the cheapest lots and returns
// the shares actually removed along with their total cost.
func (inv *Inventory) RemoveSharesMicros(sharesMicros uint64) (removedShares uint64, removedCostMicros uint64) {
	if inv == nil || sharesMicros == 0 {
		return 0, 0
	}
	rem := sharesMicros
	for rem > 0 && len(inv.lots) > 0 {
		lot := &inv.lots[0]
		if lot.SharesMicros <= rem {
			removedShares += lot.SharesMicros
			removedCostMicros += costMicrosForShares(lot.SharesMicros, lot.PriceMicros)
			rem -= lot.SharesMicros
			inv.lots = inv.lots[1:]
			continue
		}
		removedShares += rem
		removedCostMicros += costMicrosForShares(rem, lot.PriceMicros)
		lot.SharesMicros -= rem
		rem = 0
	}
	return removedShares, removedCostMicros
}
