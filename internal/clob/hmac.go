package clob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

func sanitizeBase64Secret(secret string) string {
	// Match @polymarket/clob-client behavior:
	// - accept base64url by converting '-' -> '+', '_' -> '/'
	// - drop any non-base64 chars
	secret = strings.TrimSpace(secret)
	secret = strings.ReplaceAll(secret, "-", "+")
	secret = strings.ReplaceAll(secret, "_", "/")

	var b strings.Builder
	b.Grow(len(secret))
	for i := 0; i < len(secret); i++ {
		c := secret[i]
		switch {
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c)
		case c >= 'a' && c <= 'z':
			b.WriteByte(c)
		case c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '+' || c == '/' || c == '=':
			b.WriteByte(c)
		default:
			// drop
		}
	}
	out := b.String()
	if rem := len(out) % 4; rem != 0 {
		out += strings.Repeat("=", 4-rem)
	}
	return out
}

func buildPolyHmacSignature(secret string, timestamp int64, method, requestPath string, body []byte) (string, error) {
	// Canonical message per @polymarket/clob-client:
	// message = timestamp + method + requestPath + body(optional)
	var sb strings.Builder
	sb.Grow(32 + len(method) + len(requestPath) + len(body))
	sb.WriteString(fmt.Sprintf("%d", timestamp))
	sb.WriteString(method)
	sb.WriteString(requestPath)
	if body != nil {
		sb.Write(body)
	}
	msg := []byte(sb.String())

	decoded, err := base64.StdEncoding.DecodeString(sanitizeBase64Secret(secret))
	if err != nil {
		return "", fmt.Errorf("decode base64 secret: %w", err)
	}

	mac := hmac.New(sha256.New, decoded)
	_, _ = mac.Write(msg)
	sum := mac.Sum(nil)

	sig := base64.StdEncoding.EncodeToString(sum)
	// URL-safe base64, but keep '=' suffix
	sig = strings.ReplaceAll(sig, "+", "-")
	sig = strings.ReplaceAll(sig, "/", "_")
	return sig, nil
}
