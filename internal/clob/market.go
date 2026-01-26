package clob

import (
	"context"
	"fmt"
	"math/big"
	"strings"
)

const collateralTokenDecimals = 6

type roundConfig struct {
	price  int
	size   int
	amount int
}

var roundingConfigByTickSize = map[string]roundConfig{
	"0.1":    {price: 1, size: 2, amount: 3},
	"0.01":   {price: 2, size: 2, amount: 4},
	"0.001":  {price: 3, size: 2, amount: 5},
	"0.0001": {price: 4, size: 2, amount: 6},
}

var roundDownStepByKeepDecimals = [collateralTokenDecimals + 1]*big.Int{
	0: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-0), nil), // 10^6
	1: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-1), nil), // 10^5
	2: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-2), nil), // 10^4
	3: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-3), nil), // 10^3
	4: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-4), nil), // 10^2
	5: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-5), nil), // 10^1
	6: new(big.Int).Exp(big.NewInt(10), big.NewInt(collateralTokenDecimals-6), nil), // 10^0
}

func tickScaleFromTickSize(tickSize string) (*big.Int, int, error) {
	tickSize = strings.TrimSpace(tickSize)
	rc, ok := roundingConfigByTickSize[tickSize]
	if !ok {
		return nil, 0, fmt.Errorf("unsupported tickSize %q", tickSize)
	}
	// tickScale = 10^priceDecimals
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(rc.price)), nil)
	return scale, rc.price, nil
}

func parseDecimalToUnits(s string, decimals int) (*big.Int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty decimal string")
	}
	if strings.HasPrefix(s, "-") {
		return nil, fmt.Errorf("negative not supported: %q", s)
	}

	parts := strings.SplitN(s, ".", 3)
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal: %q", s)
	}

	whole := parts[0]
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if whole == "" {
		whole = "0"
	}

	if len(frac) > decimals {
		// Truncate extra precision; the JS client ultimately enforces a fixed decimal
		// precision anyway, and under-estimating depth is safer than over-estimating.
		frac = frac[:decimals]
	}
	for len(frac) < decimals {
		frac += "0"
	}

	base := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	w, ok := new(big.Int).SetString(whole, 10)
	if !ok {
		return nil, fmt.Errorf("invalid whole part: %q", s)
	}
	w.Mul(w, base)

	if frac != "" {
		f, ok := new(big.Int).SetString(frac, 10)
		if !ok {
			return nil, fmt.Errorf("invalid fractional part: %q", s)
		}
		w.Add(w, f)
	}
	return w, nil
}

func roundDownUnits(units *big.Int, keepDecimals int) *big.Int {
	if units == nil {
		return nil
	}
	if keepDecimals >= collateralTokenDecimals {
		return new(big.Int).Set(units)
	}
	if keepDecimals < 0 {
		keepDecimals = 0
	}
	step := roundDownStepByKeepDecimals[keepDecimals]

	q := new(big.Int).Div(units, step)
	q.Mul(q, step)
	return q
}

func roundNearestUnits(units *big.Int, keepDecimals int) *big.Int {
	if units == nil {
		return nil
	}
	if keepDecimals >= collateralTokenDecimals {
		return new(big.Int).Set(units)
	}
	if keepDecimals < 0 {
		keepDecimals = 0
	}
	step := roundDownStepByKeepDecimals[keepDecimals]

	half := new(big.Int).Rsh(step, 1)
	tmp := new(big.Int).Add(units, half)
	q := new(big.Int).Div(tmp, step)
	q.Mul(q, step)
	return q
}

func (c *Client) CalculateMarketPrice(ctx context.Context, tokenID string, side Side, amountUnits *big.Int, orderType OrderType) (string, string, error) {
	book, err := c.GetOrderBook(ctx, tokenID)
	if err != nil {
		return "", "", err
	}
	if book == nil {
		return "", "", fmt.Errorf("no orderbook")
	}
	tickSize := strings.TrimSpace(book.TickSize)
	scale, priceDecimals, err := tickScaleFromTickSize(tickSize)
	if err != nil {
		return "", "", err
	}

	switch side {
	case SideBuy:
		if len(book.Asks) == 0 {
			return "", "", fmt.Errorf("no asks")
		}
		sum := new(big.Int)
		for i := len(book.Asks) - 1; i >= 0; i-- {
			p := book.Asks[i]
			priceTicks, err := parseDecimalToUnits(p.Price, priceDecimals)
			if err != nil {
				return "", "", fmt.Errorf("parse ask price %q: %w", p.Price, err)
			}
			sizeUnits, err := parseDecimalToUnits(p.Size, collateralTokenDecimals)
			if err != nil {
				return "", "", fmt.Errorf("parse ask size %q: %w", p.Size, err)
			}
			// value = size * price
			value := new(big.Int).Mul(sizeUnits, priceTicks)
			value.Div(value, scale)
			sum.Add(sum, value)
			if sum.Cmp(amountUnits) >= 0 {
				return p.Price, tickSize, nil
			}
		}
		if orderType == OrderTypeFOK {
			return "", "", fmt.Errorf("insufficient ask depth for amount")
		}
		return book.Asks[0].Price, tickSize, nil

	case SideSell:
		if len(book.Bids) == 0 {
			return "", "", fmt.Errorf("no bids")
		}
		sum := new(big.Int)
		for i := len(book.Bids) - 1; i >= 0; i-- {
			p := book.Bids[i]
			sizeUnits, err := parseDecimalToUnits(p.Size, collateralTokenDecimals)
			if err != nil {
				return "", "", fmt.Errorf("parse bid size %q: %w", p.Size, err)
			}
			sum.Add(sum, sizeUnits)
			if sum.Cmp(amountUnits) >= 0 {
				return p.Price, tickSize, nil
			}
		}
		if orderType == OrderTypeFOK {
			return "", "", fmt.Errorf("insufficient bid depth for amount")
		}
		return book.Bids[0].Price, tickSize, nil

	default:
		return "", "", fmt.Errorf("invalid side %q", side)
	}
}
