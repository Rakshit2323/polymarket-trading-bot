package arbitrage

import "testing"

func TestInventory_AddLotTotals(t *testing.T) {
	var inv Inventory

	inv.AddLot(500_000, 2*microsScale) // 2 shares @ $0.50
	inv.AddLot(500_000, 1*microsScale) // merge -> 3 shares @ $0.50
	inv.AddLot(600_000, 1*microsScale) // 1 share @ $0.60

	if got := inv.TotalSharesMicros(); got != 4*microsScale {
		t.Fatalf("total shares=%d want %d", got, 4*microsScale)
	}
	if got := inv.TotalCostMicros(); got != 2_100_000 {
		t.Fatalf("total cost=%d want %d", got, 2_100_000)
	}
}

func TestInventory_RemoveSharesMicros(t *testing.T) {
	var inv Inventory
	inv.AddLot(500_000, 2*microsScale) // 2 shares @ $0.50
	inv.AddLot(600_000, 2*microsScale) // 2 shares @ $0.60

	removedShares, removedCost := inv.RemoveSharesMicros(3 * microsScale)
	if removedShares != 3*microsScale {
		t.Fatalf("removed shares=%d want %d", removedShares, 3*microsScale)
	}
	if removedCost != 1_600_000 {
		t.Fatalf("removed cost=%d want %d", removedCost, 1_600_000)
	}
	if got := inv.TotalSharesMicros(); got != 1*microsScale {
		t.Fatalf("remaining shares=%d want %d", got, 1*microsScale)
	}
	if got := inv.TotalCostMicros(); got != 600_000 {
		t.Fatalf("remaining cost=%d want %d", got, 600_000)
	}
}
