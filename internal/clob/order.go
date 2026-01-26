package clob

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	orderbuilder "github.com/polymarket/go-order-utils/pkg/builder"
	ordermodel "github.com/polymarket/go-order-utils/pkg/model"
)

const zeroAddressHex = "0x0000000000000000000000000000000000000000"

type signedOrderPayload struct {
	DeferExec bool      `json:"deferExec"`
	Order     orderJSON `json:"order"`
	Owner     string    `json:"owner"`
	OrderType OrderType `json:"orderType"`
}

type orderJSON struct {
	Salt          int64  `json:"salt"`
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	Taker         string `json:"taker"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Expiration    string `json:"expiration"`
	Nonce         string `json:"nonce"`
	FeeRateBps    string `json:"feeRateBps"`
	Side          Side   `json:"side"`
	SignatureType int    `json:"signatureType"`
	Signature     string `json:"signature"`
}

type OrderResult struct {
	SignedOrder *ordermodel.SignedOrder
	Price       string
	TickSize    string
}

const (
	// Market orders have stricter precision requirements than the 1e6 on-chain units.
	// The CLOB API validates this and returns 400s like:
	// "market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals"
	// "sell orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals"
	marketBuyMakerMaxDecimals  = 2 // collateral (USDC) spend
	marketBuyTakerMaxDecimals  = 4 // shares receive
	marketSellMakerMaxDecimals = 2 // shares sell
	marketSellTakerMaxDecimals = 4 // collateral (USDC) receive
)

func marketOrderMaxDecimals(side Side) (makerDecimals int, takerDecimals int, err error) {
	switch side {
	case SideBuy:
		return marketBuyMakerMaxDecimals, marketBuyTakerMaxDecimals, nil
	case SideSell:
		return marketSellMakerMaxDecimals, marketSellTakerMaxDecimals, nil
	default:
		return 0, 0, fmt.Errorf("invalid side %q", side)
	}
}

func computeMarketOrderAmountsFromPrice(side Side, makerAmountUnits *big.Int, priceTicks *big.Int, priceScale *big.Int) (*big.Int, *big.Int, error) {
	if makerAmountUnits == nil || makerAmountUnits.Sign() <= 0 {
		return nil, nil, fmt.Errorf("maker amount must be > 0")
	}
	if priceTicks == nil || priceTicks.Sign() <= 0 {
		return nil, nil, fmt.Errorf("priceTicks must be > 0")
	}
	if priceScale == nil || priceScale.Sign() <= 0 {
		return nil, nil, fmt.Errorf("priceScale must be > 0")
	}

	makerDecimals, takerDecimals, err := marketOrderMaxDecimals(side)
	if err != nil {
		return nil, nil, err
	}

	// For sells, round down maker (shares) so we never exceed inventory.
	makerRounded := roundNearestUnits(makerAmountUnits, makerDecimals)
	if side == SideSell {
		makerRounded = roundDownUnits(makerAmountUnits, makerDecimals)
	}
	if makerRounded == nil || makerRounded.Sign() <= 0 {
		return nil, nil, fmt.Errorf("maker amount rounds to 0")
	}

	switch side {
	case SideBuy:
		// BUY: maker = collateral, taker = shares
		shares := new(big.Int).Mul(makerRounded, priceScale)
		shares.Div(shares, priceTicks)
		takerRounded := roundDownUnits(shares, takerDecimals)
		if takerRounded == nil || takerRounded.Sign() <= 0 {
			return nil, nil, fmt.Errorf("taker amount rounds to 0")
		}
		return makerRounded, takerRounded, nil
	case SideSell:
		// SELL: maker = shares, taker = collateral
		dollars := new(big.Int).Mul(makerRounded, priceTicks)
		dollars.Div(dollars, priceScale)
		takerRounded := roundDownUnits(dollars, takerDecimals)
		if takerRounded == nil || takerRounded.Sign() <= 0 {
			return nil, nil, fmt.Errorf("taker amount rounds to 0")
		}
		return makerRounded, takerRounded, nil
	default:
		// marketOrderMaxDecimals already guards this, but keep behavior robust if called directly.
		return nil, nil, fmt.Errorf("invalid side %q", side)
	}
}

func (c *Client) CreateSignedMarketOrder(
	ctx context.Context,
	tokenID string,
	side Side,
	amountUnits *big.Int,
	orderType OrderType,
	useServerTime bool,
	saltGenerator func() int64,
) (*OrderResult, error) {
	if amountUnits == nil || amountUnits.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be > 0")
	}

	makerDecimals, _, err := marketOrderMaxDecimals(side)
	if err != nil {
		return nil, err
	}
	// For sells, never round up the maker (shares) amount; it can exceed balance.
	rounder := roundNearestUnits
	if side == SideSell {
		rounder = roundDownUnits
	}
	amountRounded := rounder(amountUnits, makerDecimals)
	if amountRounded == nil || amountRounded.Sign() <= 0 {
		return nil, fmt.Errorf("amount rounds to 0 at %d decimals", makerDecimals)
	}

	// Market price selection uses the order book.
	price, tickSize, err := c.CalculateMarketPrice(ctx, tokenID, side, amountRounded, orderType)
	if err != nil {
		return nil, err
	}

	scale, priceDecimals, err := tickScaleFromTickSize(tickSize)
	if err != nil {
		return nil, err
	}
	priceTicks, err := parseDecimalToUnits(price, priceDecimals)
	if err != nil {
		return nil, fmt.Errorf("parse market price %q: %w", price, err)
	}
	if priceTicks.Sign() <= 0 {
		return nil, fmt.Errorf("invalid price %q", price)
	}

	var sideEnum ordermodel.Side
	switch side {
	case SideBuy:
		sideEnum = ordermodel.BUY
	case SideSell:
		sideEnum = ordermodel.SELL
	default:
		return nil, fmt.Errorf("invalid side %q", side)
	}

	makerAmountUnits, takerAmountUnits, err := computeMarketOrderAmountsFromPrice(side, amountRounded, priceTicks, scale)
	if err != nil {
		return nil, err
	}

	feeBps, err := c.GetFeeRateBps(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	negRisk, err := c.GetNegRisk(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	contract := ordermodel.CTFExchange
	if negRisk {
		contract = ordermodel.NegRiskCTFExchange
	}

	od := &ordermodel.OrderData{
		Maker:         c.funder.Hex(),
		Taker:         zeroAddressHex,
		TokenId:       tokenID,
		MakerAmount:   makerAmountUnits.String(),
		TakerAmount:   takerAmountUnits.String(),
		FeeRateBps:    strconv.Itoa(feeBps),
		Nonce:         "0",
		Signer:        c.signer.Hex(),
		Expiration:    "0",
		Side:          sideEnum,
		SignatureType: ordermodel.SignatureType(c.signatureTy),
	}

	signed, err := signOrder(c.chainID, c.privateKey, od, contract, saltGenerator)
	if err != nil {
		return nil, err
	}

	return &OrderResult{SignedOrder: signed, Price: price, TickSize: tickSize}, nil
}

func (c *Client) CreateSignedMarketOrderWithSlippage(
	ctx context.Context,
	tokenID string,
	side Side,
	amountUnits *big.Int,
	orderType OrderType,
	useServerTime bool,
	slippageBps int,
	saltGenerator func() int64,
) (*OrderResult, error) {
	if slippageBps < 0 {
		return nil, fmt.Errorf("slippage bps must be >= 0")
	}
	if slippageBps == 0 {
		return c.CreateSignedMarketOrder(ctx, tokenID, side, amountUnits, orderType, useServerTime, saltGenerator)
	}
	if amountUnits == nil || amountUnits.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be > 0")
	}

	makerDecimals, _, err := marketOrderMaxDecimals(side)
	if err != nil {
		return nil, err
	}
	// For sells, never round up the maker (shares) amount; it can exceed balance.
	rounder := roundNearestUnits
	if side == SideSell {
		rounder = roundDownUnits
	}
	amountRounded := rounder(amountUnits, makerDecimals)
	if amountRounded == nil || amountRounded.Sign() <= 0 {
		return nil, fmt.Errorf("amount rounds to 0 at %d decimals", makerDecimals)
	}

	price, tickSize, err := c.CalculateMarketPrice(ctx, tokenID, side, amountRounded, orderType)
	if err != nil {
		return nil, err
	}

	scale, priceDecimals, err := tickScaleFromTickSize(tickSize)
	if err != nil {
		return nil, err
	}
	priceTicks, err := parseDecimalToUnits(price, priceDecimals)
	if err != nil {
		return nil, fmt.Errorf("parse market price %q: %w", price, err)
	}
	if priceTicks.Sign() <= 0 {
		return nil, fmt.Errorf("invalid price %q", price)
	}

	bpsScale := big.NewInt(10_000)
	adjPriceTicks := new(big.Int).Set(priceTicks)
	switch side {
	case SideBuy:
		adjPriceTicks.Mul(adjPriceTicks, big.NewInt(int64(10_000+slippageBps)))
		adjPriceTicks.Add(adjPriceTicks, new(big.Int).Sub(bpsScale, big.NewInt(1)))
		adjPriceTicks.Div(adjPriceTicks, bpsScale)
	case SideSell:
		if slippageBps >= 10_000 {
			return nil, fmt.Errorf("slippage bps must be < 10000 for sells")
		}
		adjPriceTicks.Mul(adjPriceTicks, big.NewInt(int64(10_000-slippageBps)))
		adjPriceTicks.Div(adjPriceTicks, bpsScale)
	default:
		return nil, fmt.Errorf("invalid side %q", side)
	}
	if adjPriceTicks.Sign() <= 0 {
		return nil, fmt.Errorf("slippage-adjusted price <= 0")
	}

	var sideEnum ordermodel.Side
	switch side {
	case SideBuy:
		sideEnum = ordermodel.BUY
	case SideSell:
		sideEnum = ordermodel.SELL
	default:
		return nil, fmt.Errorf("invalid side %q", side)
	}

	makerAmountUnits, takerAmountUnits, err := computeMarketOrderAmountsFromPrice(side, amountRounded, adjPriceTicks, scale)
	if err != nil {
		return nil, err
	}

	feeBps, err := c.GetFeeRateBps(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	negRisk, err := c.GetNegRisk(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	contract := ordermodel.CTFExchange
	if negRisk {
		contract = ordermodel.NegRiskCTFExchange
	}

	od := &ordermodel.OrderData{
		Maker:         c.funder.Hex(),
		Taker:         zeroAddressHex,
		TokenId:       tokenID,
		MakerAmount:   makerAmountUnits.String(),
		TakerAmount:   takerAmountUnits.String(),
		FeeRateBps:    strconv.Itoa(feeBps),
		Nonce:         "0",
		Signer:        c.signer.Hex(),
		Expiration:    "0",
		Side:          sideEnum,
		SignatureType: ordermodel.SignatureType(c.signatureTy),
	}

	signed, err := signOrder(c.chainID, c.privateKey, od, contract, saltGenerator)
	if err != nil {
		return nil, err
	}

	adjPrice := formatDecimalUnits(adjPriceTicks, priceDecimals)
	return &OrderResult{SignedOrder: signed, Price: adjPrice, TickSize: tickSize}, nil
}

func (c *Client) CreateSignedExactOrder(
	ctx context.Context,
	tokenID string,
	side Side,
	makerAmountUnits *big.Int,
	takerAmountUnits *big.Int,
	saltGenerator func() int64,
) (*OrderResult, error) {
	if makerAmountUnits == nil || makerAmountUnits.Sign() <= 0 {
		return nil, fmt.Errorf("maker amount must be > 0")
	}
	if takerAmountUnits == nil || takerAmountUnits.Sign() <= 0 {
		return nil, fmt.Errorf("taker amount must be > 0")
	}

	tickSize, err := c.GetTickSize(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	tickSize = strings.TrimSpace(tickSize)
	scale, priceDecimals, err := tickScaleFromTickSize(tickSize)
	if err != nil {
		return nil, err
	}

	// Derive the effective price for logging: collateral per share, truncated to tick precision.
	var priceTicks big.Int
	switch side {
	case SideBuy:
		// BUY: maker = collateral, taker = shares => price = maker/taker
		priceTicks.Mul(makerAmountUnits, scale)
		priceTicks.Div(&priceTicks, takerAmountUnits)
	case SideSell:
		// SELL: maker = shares, taker = collateral => price = taker/maker
		priceTicks.Mul(takerAmountUnits, scale)
		priceTicks.Div(&priceTicks, makerAmountUnits)
	default:
		return nil, fmt.Errorf("invalid side %q", side)
	}

	if priceTicks.Sign() <= 0 {
		return nil, fmt.Errorf("invalid derived price")
	}

	// Quantize amounts to satisfy market-order precision rails enforced by the CLOB API.
	// We preserve the derived tick price, round down the maker side to the allowed decimals,
	// and recompute the taker side from price ticks so the resulting order is not stricter
	// than the derived price.
	roundedMaker, roundedTaker, err := computeMarketOrderAmountsFromPrice(side, makerAmountUnits, &priceTicks, scale)
	if err != nil {
		return nil, err
	}
	makerAmountUnits = roundedMaker
	takerAmountUnits = roundedTaker

	// Recompute the implied price after quantization for logging.
	var outPriceTicks big.Int
	switch side {
	case SideBuy:
		outPriceTicks.Mul(makerAmountUnits, scale)
		outPriceTicks.Div(&outPriceTicks, takerAmountUnits)
	case SideSell:
		outPriceTicks.Mul(takerAmountUnits, scale)
		outPriceTicks.Div(&outPriceTicks, makerAmountUnits)
	}
	price := formatDecimalUnits(&outPriceTicks, priceDecimals)

	feeBps, err := c.GetFeeRateBps(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	negRisk, err := c.GetNegRisk(ctx, tokenID)
	if err != nil {
		return nil, err
	}

	contract := ordermodel.CTFExchange
	if negRisk {
		contract = ordermodel.NegRiskCTFExchange
	}

	var sideEnum ordermodel.Side
	switch side {
	case SideBuy:
		sideEnum = ordermodel.BUY
	case SideSell:
		sideEnum = ordermodel.SELL
	default:
		return nil, fmt.Errorf("invalid side %q", side)
	}

	od := &ordermodel.OrderData{
		Maker:         c.funder.Hex(),
		Taker:         zeroAddressHex,
		TokenId:       tokenID,
		MakerAmount:   makerAmountUnits.String(),
		TakerAmount:   takerAmountUnits.String(),
		FeeRateBps:    strconv.Itoa(feeBps),
		Nonce:         "0",
		Signer:        c.signer.Hex(),
		Expiration:    "0",
		Side:          sideEnum,
		SignatureType: ordermodel.SignatureType(c.signatureTy),
	}

	signed, err := signOrder(c.chainID, c.privateKey, od, contract, saltGenerator)
	if err != nil {
		return nil, err
	}
	return &OrderResult{SignedOrder: signed, Price: price, TickSize: tickSize}, nil
}

func signOrder(chainID int64, pk *ecdsa.PrivateKey, od *ordermodel.OrderData, contract ordermodel.VerifyingContract, saltGen func() int64) (*ordermodel.SignedOrder, error) {
	b := orderbuilder.NewExchangeOrderBuilderImpl(big.NewInt(chainID), saltGen)
	return b.BuildSignedOrder(pk, od, contract)
}

func (c *Client) PostSignedOrder(
	ctx context.Context,
	order *ordermodel.SignedOrder,
	orderType OrderType,
	deferExec bool,
	useServerTime bool,
) (map[string]any, []byte, error) {
	if order == nil {
		return nil, nil, fmt.Errorf("order required")
	}

	body, err := c.BuildPostOrderBody(order, orderType, deferExec)
	if err != nil {
		return nil, nil, err
	}

	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodPost, "/order", body)
	if err != nil {
		return nil, nil, err
	}

	var resp map[string]any
	if err := c.doJSONBody(ctx, http.MethodPost, "/order", nil, headers, body, &resp); err != nil {
		return nil, body, err
	}
	return resp, body, nil
}

func (c *Client) BuildPostOrderBody(order *ordermodel.SignedOrder, orderType OrderType, deferExec bool) ([]byte, error) {
	if order == nil {
		return nil, fmt.Errorf("order required")
	}
	c.mu.RLock()
	creds := c.creds
	c.mu.RUnlock()

	owner := ""
	if creds != nil {
		owner = creds.Key
	}

	payload := signedOrderPayload{
		DeferExec: deferExec,
		Owner:     owner,
		OrderType: orderType,
		Order: orderJSON{
			Salt:          order.Salt.Int64(),
			Maker:         order.Maker.Hex(),
			Signer:        order.Signer.Hex(),
			Taker:         order.Taker.Hex(),
			TokenID:       order.TokenId.String(),
			MakerAmount:   order.MakerAmount.String(),
			TakerAmount:   order.TakerAmount.String(),
			Expiration:    order.Expiration.String(),
			Nonce:         order.Nonce.String(),
			FeeRateBps:    order.FeeRateBps.String(),
			Side:          sideToString(order.Side),
			SignatureType: int(order.SignatureType.Int64()),
			Signature:     "0x" + fmt.Sprintf("%x", order.Signature),
		},
	}
	return json.Marshal(payload)
}

func sideToString(v *big.Int) Side {
	if v == nil {
		return SideBuy
	}
	if v.Int64() == int64(ordermodel.SELL) {
		return SideSell
	}
	return SideBuy
}

// BatchOrderResult represents the result of a single order in a batch request.
type BatchOrderResult struct {
	Success     bool     `json:"success"`
	ErrorMsg    string   `json:"errorMsg,omitempty"`
	OrderID     string   `json:"orderId,omitempty"`
	OrderHashes []string `json:"orderHashes,omitempty"`
}

// Filled returns true if the order was actually filled.
// Note: Polymarket API returns success=true even for killed FOK orders,
// so we must also check that errorMsg is empty to confirm a fill.
func (r BatchOrderResult) Filled() bool {
	return r.Success && r.ErrorMsg == ""
}

// PostSignedOrders submits multiple orders in a single batch request.
// Up to 15 orders can be submitted at once per Polymarket API limits.
func (c *Client) PostSignedOrders(
	ctx context.Context,
	orders []*ordermodel.SignedOrder,
	orderTypes []OrderType,
	useServerTime bool,
) ([]BatchOrderResult, []byte, error) {
	if len(orders) == 0 {
		return nil, nil, fmt.Errorf("no orders provided")
	}
	if len(orders) != len(orderTypes) {
		return nil, nil, fmt.Errorf("orders and orderTypes length mismatch")
	}
	if len(orders) > 15 {
		return nil, nil, fmt.Errorf("batch limit is 15 orders, got %d", len(orders))
	}

	c.mu.RLock()
	creds := c.creds
	c.mu.RUnlock()

	owner := ""
	if creds != nil {
		owner = creds.Key
	}

	// Build the batch payload as an array of order objects.
	payloads := make([]signedOrderPayload, len(orders))
	for i, order := range orders {
		if order == nil {
			return nil, nil, fmt.Errorf("order at index %d is nil", i)
		}
		payloads[i] = signedOrderPayload{
			DeferExec: false,
			Owner:     owner,
			OrderType: orderTypes[i],
			Order: orderJSON{
				Salt:          order.Salt.Int64(),
				Maker:         order.Maker.Hex(),
				Signer:        order.Signer.Hex(),
				Taker:         order.Taker.Hex(),
				TokenID:       order.TokenId.String(),
				MakerAmount:   order.MakerAmount.String(),
				TakerAmount:   order.TakerAmount.String(),
				Expiration:    order.Expiration.String(),
				Nonce:         order.Nonce.String(),
				FeeRateBps:    order.FeeRateBps.String(),
				Side:          sideToString(order.Side),
				SignatureType: int(order.SignatureType.Int64()),
				Signature:     "0x" + fmt.Sprintf("%x", order.Signature),
			},
		}
	}

	body, err := json.Marshal(payloads)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal batch orders: %w", err)
	}

	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodPost, "/orders", body)
	if err != nil {
		return nil, nil, err
	}

	var resp []BatchOrderResult
	respBody, err := c.doJSONBodyWithResponse(ctx, http.MethodPost, "/orders", nil, headers, body, &resp)
	if err != nil {
		return nil, respBody, err
	}
	return resp, respBody, nil
}

func formatDecimalUnits(units *big.Int, decimals int) string {
	if units == nil {
		return "0"
	}
	if decimals <= 0 {
		return units.String()
	}

	s := units.String()
	if s == "" {
		return "0"
	}

	// Left-pad so we always have at least one digit before the decimal point.
	if len(s) <= decimals {
		s = strings.Repeat("0", decimals-len(s)+1) + s
	}
	i := len(s) - decimals
	out := s[:i] + "." + s[i:]
	out = strings.TrimRight(out, "0")
	out = strings.TrimRight(out, ".")
	if out == "" {
		return "0"
	}
	return out
}
