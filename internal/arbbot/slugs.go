package arbbot

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultSlugsFileName = "slugs.txt"

func parseSlugList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parts := strings.FieldsFunc(raw, func(r rune) bool {
		switch r {
		case ',', ';', '\n', '\t', ' ':
			return true
		default:
			return false
		}
	})

	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func readSlugsFile(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var b strings.Builder
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		line = stripLineComment(line)
		if line == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return parseSlugList(b.String()), nil
}

func stripLineComment(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}

	if idx := strings.IndexByte(line, '#'); idx >= 0 {
		line = strings.TrimSpace(line[:idx])
	}

	if idx := strings.Index(line, "//"); idx >= 0 {
		// Treat // as a comment delimiter only if it starts the line or is preceded by whitespace.
		if idx == 0 || line[idx-1] == ' ' || line[idx-1] == '\t' {
			line = strings.TrimSpace(line[:idx])
		}
	}

	return strings.TrimSpace(line)
}

func defaultSlugsFileIfPresent() string {
	// 1) Fast-path: check cwd for ./slugs.txt (nice short logs).
	if isRegularFile(defaultSlugsFileName) {
		return defaultSlugsFileName
	}

	// 2) If we're running from a subdir of the repo/module, walk upward until we
	// hit a go.mod (module root) and look for slugs.txt on the way. This makes
	// `go run .` from cmd/arbitrage work without extra flags.
	cwd, err := os.Getwd()
	if err == nil && cwd != "" {
		if p := findUpwardUntilGoMod(cwd, defaultSlugsFileName); p != "" {
			return p
		}
	}

	// 3) Finally, check next to the executable (useful for running a built binary).
	exe, err := os.Executable()
	if err == nil && strings.TrimSpace(exe) != "" {
		if p := filepath.Join(filepath.Dir(exe), defaultSlugsFileName); isRegularFile(p) {
			return p
		}
	}

	return ""
}

func isRegularFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info == nil {
		return false
	}
	return info.Mode().IsRegular()
}

func findUpwardUntilGoMod(startDir, fileName string) string {
	dir := filepath.Clean(startDir)
	for {
		if strings.TrimSpace(dir) == "" || dir == "." {
			return ""
		}

		p := filepath.Join(dir, fileName)
		if isRegularFile(p) {
			return p
		}

		// Stop at the first go.mod (treat as module root).
		if isRegularFile(filepath.Join(dir, "go.mod")) {
			return ""
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
