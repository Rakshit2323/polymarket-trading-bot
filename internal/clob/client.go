package clob

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type OrderType string

const (
	OrderTypeGTC OrderType = "GTC"
	OrderTypeFOK OrderType = "FOK"
	OrderTypeGTD OrderType = "GTD"
	OrderTypeFAK OrderType = "FAK"
)

type ApiKeyCreds struct {
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

type apiKeyRaw struct {
	APIKey     string `json:"apiKey"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

type OrderBookSummary struct {
	Market    string         `json:"market"`
	AssetID   string         `json:"asset_id"`
	Timestamp string         `json:"timestamp"`
	Bids      []OrderSummary `json:"bids"`
	Asks      []OrderSummary `json:"asks"`
	MinOrder  string         `json:"min_order_size"`
	TickSize  string         `json:"tick_size"`
	NegRisk   bool           `json:"neg_risk"`
	Hash      string         `json:"hash"`
}

type OrderSummary struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type decimalString string

func (d *decimalString) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		*d = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*d = decimalString(canonicalDecimalString(s))
		return nil
	}
	*d = decimalString(canonicalDecimalString(string(b)))
	return nil
}

func canonicalDecimalString(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, ".") {
		s = "0" + s
	}
	if strings.Contains(s, ".") {
		parts := strings.SplitN(s, ".", 2)
		whole := parts[0]
		frac := strings.TrimRight(parts[1], "0")
		if frac == "" {
			return whole
		}
		return whole + "." + frac
	}
	return s
}

type tickSizeResp struct {
	MinimumTickSize decimalString `json:"minimum_tick_size"`
}

type feeRateResp struct {
	BaseFee int `json:"base_fee"`
}

type negRiskResp struct {
	NegRisk bool `json:"neg_risk"`
}

type Client struct {
	host        string
	httpClient  *http.Client
	chainID     int64
	privateKey  *ecdsa.PrivateKey
	signer      common.Address
	funder      common.Address
	signatureTy int // 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE

	mu       sync.RWMutex
	creds    *ApiKeyCreds
	tickSize map[string]string
	feeRate  map[string]int
	negRisk  map[string]bool
}

func NewClient(host string, chainID int64, privateKey *ecdsa.PrivateKey, funder common.Address, signatureType int) (*Client, error) {
	if host == "" {
		host = "https://clob.polymarket.com"
	}
	host = strings.TrimRight(host, "/")
	if !strings.HasPrefix(host, "http") {
		return nil, fmt.Errorf("clob host must be http(s), got %q", host)
	}
	if privateKey == nil {
		return nil, fmt.Errorf("private key required")
	}
	signer := crypto.PubkeyToAddress(privateKey.PublicKey)
	if (funder == common.Address{}) {
		funder = signer
	}

	return &Client{
		host:        host,
		httpClient:  &http.Client{Timeout: 15 * time.Second},
		chainID:     chainID,
		privateKey:  privateKey,
		signer:      signer,
		funder:      funder,
		signatureTy: signatureType,
		tickSize:    make(map[string]string),
		feeRate:     make(map[string]int),
		negRisk:     make(map[string]bool),
	}, nil
}

func (c *Client) SignerAddress() common.Address { return c.signer }
func (c *Client) FunderAddress() common.Address { return c.funder }
func (c *Client) ChainID() int64                { return c.chainID }
func (c *Client) SignatureType() int            { return c.signatureTy }

func (c *Client) SetApiCreds(creds ApiKeyCreds) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creds = &creds
}

func (c *Client) HasApiCreds() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.creds != nil && c.creds.Key != "" && c.creds.Secret != "" && c.creds.Passphrase != ""
}

func (c *Client) GetServerTime(ctx context.Context) (int64, error) {
	var ts int64
	if err := c.doJSON(ctx, http.MethodGet, "/time", nil, nil, &ts); err != nil {
		return 0, err
	}
	return ts, nil
}

func (c *Client) GetTickSize(ctx context.Context, tokenID string) (string, error) {
	c.mu.RLock()
	if v, ok := c.tickSize[tokenID]; ok && v != "" {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	params := url.Values{"token_id": []string{tokenID}}
	var resp tickSizeResp
	if err := c.doJSON(ctx, http.MethodGet, "/tick-size", params, nil, &resp); err != nil {
		return "", err
	}
	tickSize := string(resp.MinimumTickSize)
	if tickSize == "" {
		return "", fmt.Errorf("tick size missing in response")
	}

	c.mu.Lock()
	c.tickSize[tokenID] = tickSize
	c.mu.Unlock()
	return tickSize, nil
}

func (c *Client) GetFeeRateBps(ctx context.Context, tokenID string) (int, error) {
	c.mu.RLock()
	if v, ok := c.feeRate[tokenID]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	params := url.Values{"token_id": []string{tokenID}}
	var resp feeRateResp
	if err := c.doJSON(ctx, http.MethodGet, "/fee-rate", params, nil, &resp); err != nil {
		return 0, err
	}

	c.mu.Lock()
	c.feeRate[tokenID] = resp.BaseFee
	c.mu.Unlock()
	return resp.BaseFee, nil
}

func (c *Client) GetNegRisk(ctx context.Context, tokenID string) (bool, error) {
	c.mu.RLock()
	if v, ok := c.negRisk[tokenID]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	params := url.Values{"token_id": []string{tokenID}}
	var resp negRiskResp
	if err := c.doJSON(ctx, http.MethodGet, "/neg-risk", params, nil, &resp); err != nil {
		return false, err
	}

	c.mu.Lock()
	c.negRisk[tokenID] = resp.NegRisk
	c.mu.Unlock()
	return resp.NegRisk, nil
}

func (c *Client) GetOrderBook(ctx context.Context, tokenID string) (*OrderBookSummary, error) {
	params := url.Values{"token_id": []string{tokenID}}
	var book OrderBookSummary
	if err := c.doJSON(ctx, http.MethodGet, "/book", params, nil, &book); err != nil {
		return nil, err
	}
	return &book, nil
}

func (c *Client) CreateOrDeriveApiKey(ctx context.Context, nonce uint64, useServerTime bool) (ApiKeyCreds, error) {
	// Derive first to avoid NONCE_ALREADY_USED failures on create.
	if creds, err := c.DeriveApiKey(ctx, nonce, useServerTime); err == nil && creds.Key != "" {
		return creds, nil
	}
	return c.CreateApiKey(ctx, nonce, useServerTime)
}

func (c *Client) CreateApiKey(ctx context.Context, nonce uint64, useServerTime bool) (ApiKeyCreds, error) {
	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	headers, err := c.l1Headers(ts, nonce)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	var resp apiKeyRaw
	if err := c.doJSON(ctx, http.MethodPost, "/auth/api-key", nil, headers, &resp); err != nil {
		return ApiKeyCreds{}, err
	}
	return ApiKeyCreds{Key: resp.APIKey, Secret: resp.Secret, Passphrase: resp.Passphrase}, nil
}

func (c *Client) DeriveApiKey(ctx context.Context, nonce uint64, useServerTime bool) (ApiKeyCreds, error) {
	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	headers, err := c.l1Headers(ts, nonce)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	var resp apiKeyRaw
	if err := c.doJSON(ctx, http.MethodGet, "/auth/derive-api-key", nil, headers, &resp); err != nil {
		return ApiKeyCreds{}, err
	}
	return ApiKeyCreds{Key: resp.APIKey, Secret: resp.Secret, Passphrase: resp.Passphrase}, nil
}

func (c *Client) timestampForAuth(ctx context.Context, useServerTime bool) (int64, error) {
	if !useServerTime {
		return time.Now().Unix(), nil
	}
	return c.GetServerTime(ctx)
}

func (c *Client) l1Headers(timestamp int64, nonce uint64) (http.Header, error) {
	sig, err := buildClobEip712Signature(c.privateKey, c.signer, c.chainID, timestamp, nonce)
	if err != nil {
		return nil, err
	}
	h := make(http.Header)
	h.Set("POLY_ADDRESS", c.signer.Hex())
	h.Set("POLY_SIGNATURE", sig)
	h.Set("POLY_TIMESTAMP", strconv.FormatInt(timestamp, 10))
	h.Set("POLY_NONCE", strconv.FormatUint(nonce, 10))
	return h, nil
}

func (c *Client) l2Headers(timestamp int64, method, requestPath string, body []byte) (http.Header, error) {
	c.mu.RLock()
	creds := c.creds
	c.mu.RUnlock()
	if creds == nil {
		return nil, fmt.Errorf("api creds not set")
	}
	sig, err := buildPolyHmacSignature(creds.Secret, timestamp, method, requestPath, body)
	if err != nil {
		return nil, err
	}
	h := make(http.Header)
	h.Set("POLY_ADDRESS", c.signer.Hex())
	h.Set("POLY_SIGNATURE", sig)
	h.Set("POLY_TIMESTAMP", strconv.FormatInt(timestamp, 10))
	h.Set("POLY_API_KEY", creds.Key)
	h.Set("POLY_PASSPHRASE", creds.Passphrase)
	return h, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, params url.Values, headers http.Header, out any) error {
	u := c.host + path
	if params != nil && len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}
	if headers != nil {
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("clob %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode %s response: %w (body=%s)", path, err, strings.TrimSpace(string(b)))
	}
	return nil
}

func (c *Client) doJSONBody(ctx context.Context, method, path string, params url.Values, headers http.Header, body []byte, out any) error {
	_, err := c.doJSONBodyWithResponse(ctx, method, path, params, headers, body, out)
	return err
}

func (c *Client) doJSONBodyWithResponse(ctx context.Context, method, path string, params url.Values, headers http.Header, body []byte, out any) ([]byte, error) {
	u := c.host + path
	if params != nil && len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if headers != nil {
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return b, fmt.Errorf("clob %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return b, nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return b, fmt.Errorf("decode %s response: %w (body=%s)", path, err, strings.TrimSpace(string(b)))
	}
	return b, nil
}
