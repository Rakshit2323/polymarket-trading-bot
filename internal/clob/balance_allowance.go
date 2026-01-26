package clob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	balanceAllowancePath       = "/balance-allowance"
	balanceAllowanceUpdatePath = "/balance-allowance/update"
)

// BalanceAllowanceParams controls optional query filters for balance/allowance endpoints.
// AssetType can be "COLLATERAL" or "CONDITIONAL".
// SignatureType < 0 uses the client's configured signature type.
type BalanceAllowanceParams struct {
	AssetType     string
	TokenID       string
	SignatureType int
}

func (c *Client) GetBalanceAllowance(ctx context.Context, params *BalanceAllowanceParams, useServerTime bool) (map[string]any, error) {
	return c.fetchBalanceAllowance(ctx, balanceAllowancePath, params, useServerTime)
}

func (c *Client) UpdateBalanceAllowance(ctx context.Context, params *BalanceAllowanceParams, useServerTime bool) (map[string]any, error) {
	return c.fetchBalanceAllowance(ctx, balanceAllowanceUpdatePath, params, useServerTime)
}

func (c *Client) fetchBalanceAllowance(ctx context.Context, path string, params *BalanceAllowanceParams, useServerTime bool) (map[string]any, error) {
	if !c.HasApiCreds() {
		return nil, fmt.Errorf("api creds not configured")
	}

	var p BalanceAllowanceParams
	if params != nil {
		p = *params
	} else {
		p.SignatureType = -1
	}
	p.AssetType = strings.TrimSpace(p.AssetType)
	p.TokenID = strings.TrimSpace(p.TokenID)
	if p.SignatureType < 0 {
		p.SignatureType = c.signatureTy
	}

	q := url.Values{}
	if p.AssetType != "" {
		q.Set("asset_type", p.AssetType)
	}
	if p.TokenID != "" {
		q.Set("token_id", p.TokenID)
	}
	q.Set("signature_type", strconv.Itoa(p.SignatureType))

	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	body, err := c.doJSONBodyWithResponse(ctx, http.MethodGet, path, q, headers, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}, nil
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode %s response: %w (body=%s)", path, err, strings.TrimSpace(string(body)))
	}
	return resp, nil
}
