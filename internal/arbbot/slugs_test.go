package arbbot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadSlugsFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "slugs.txt")
	content := `# comment line

btc-updown-15m
eth-updown-15m # inline comment
// full line comment

sol-updown-4h
xrp-updown-4h   // trailing comment

bitcoin-up-or-down
`
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	slugs, err := readSlugsFile(p)
	if err != nil {
		t.Fatalf("readSlugsFile: %v", err)
	}

	want := []string{
		"btc-updown-15m",
		"eth-updown-15m",
		"sol-updown-4h",
		"xrp-updown-4h",
		"bitcoin-up-or-down",
	}
	if len(slugs) != len(want) {
		t.Fatalf("unexpected slugs count: got %d want %d (%v)", len(slugs), len(want), slugs)
	}
	for i := range want {
		if slugs[i] != want[i] {
			t.Fatalf("slug[%d]: got %q want %q (all=%v)", i, slugs[i], want[i], slugs)
		}
	}
}

func TestDefaultSlugsFileIfPresent(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	if got := defaultSlugsFileIfPresent(); got != "" {
		t.Fatalf("expected no default file, got %q", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "slugs.txt"), []byte("btc-updown-15m\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := defaultSlugsFileIfPresent(); got != "slugs.txt" {
		t.Fatalf("expected default file, got %q", got)
	}
}

func TestDefaultSlugsFileIfPresent_SearchParentsUpToGoMod(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}

	root := t.TempDir()
	// A minimal go.mod marks the module root (search stop point).
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/test\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(go.mod): %v", err)
	}

	want := filepath.Join(root, "slugs.txt")
	if err := os.WriteFile(want, []byte("btc-updown-15m\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(slugs.txt): %v", err)
	}

	sub := filepath.Join(root, "cmd", "arbitrage")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Chdir(sub); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	got := defaultSlugsFileIfPresent()
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
