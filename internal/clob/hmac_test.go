package clob

import "testing"

func TestBuildPolyHmacSignature(t *testing.T) {
	sig, err := buildPolyHmacSignature(
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		1000000,
		"test-sign",
		"/orders",
		[]byte(`{"hash": "0x123"}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = "ZwAdJKvoYRlEKDkNMwd5BuwNNtg93kNaR_oU2HrfVvc="
	if sig != want {
		t.Fatalf("signature mismatch: got %q want %q", sig, want)
	}
}

func TestBuildPolyHmacSignature_Base64URLCompat(t *testing.T) {
	base64Sig, err := buildPolyHmacSignature(
		"++/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		1000000,
		"test-sign",
		"/orders",
		[]byte(`{"hash": "0x123"}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	base64URLSig, err := buildPolyHmacSignature(
		"--_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		1000000,
		"test-sign",
		"/orders",
		[]byte(`{"hash": "0x123"}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if base64Sig != base64URLSig {
		t.Fatalf("expected base64url and base64 to match: %q vs %q", base64URLSig, base64Sig)
	}
}

func TestBuildPolyHmacSignature_IgnoresInvalidSymbols(t *testing.T) {
	sig, err := buildPolyHmacSignature(
		"AAAAAAAAA^^AAAAAAAA<>AAAAA||AAAAAAAAAAAAAAAAAAAAA=",
		1000000,
		"test-sign",
		"/orders",
		[]byte(`{"hash": "0x123"}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = "ZwAdJKvoYRlEKDkNMwd5BuwNNtg93kNaR_oU2HrfVvc="
	if sig != want {
		t.Fatalf("signature mismatch: got %q want %q", sig, want)
	}
}
