package arbitrage

import (
	"fmt"
	"math"
	"strings"
)

const microsScale = uint64(1_000_000)

// ParseMicros parses a base-10 decimal string into integer "micro" units (1e6 scale).
//
// Examples:
//   - "1"        -> 1_000_000
//   - "0.55"     ->   550_000
//   - ".5"       ->   500_000
//   - "1.000001" -> 1_000_001
//
// If the input has more than 6 fractional digits, extra digits are truncated (not rounded).
func ParseMicros(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty decimal")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("negative not supported: %q", s)
	}
	if strings.HasPrefix(s, "+") {
		s = strings.TrimSpace(s[1:])
		if s == "" {
			return 0, fmt.Errorf("invalid decimal")
		}
	}

	var whole uint64
	var frac uint64
	fracDigits := 0
	seenDot := false
	seenDigit := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.':
			if seenDot {
				return 0, fmt.Errorf("invalid decimal %q", s)
			}
			seenDot = true
		case c >= '0' && c <= '9':
			d := uint64(c - '0')
			seenDigit = true
			if !seenDot {
				if whole > (math.MaxUint64-d)/10 {
					return 0, fmt.Errorf("decimal overflow %q", s)
				}
				whole = whole*10 + d
				continue
			}
			if fracDigits < 6 {
				if frac > (math.MaxUint64-d)/10 {
					return 0, fmt.Errorf("decimal overflow %q", s)
				}
				frac = frac*10 + d
				fracDigits++
			}
			// Truncate extra fractional digits.
		default:
			return 0, fmt.Errorf("invalid decimal %q", s)
		}
	}

	if !seenDigit {
		return 0, fmt.Errorf("invalid decimal %q", s)
	}
	for fracDigits < 6 {
		frac *= 10
		fracDigits++
	}
	if whole > (math.MaxUint64-frac)/microsScale {
		return 0, fmt.Errorf("decimal overflow %q", s)
	}
	return whole*microsScale + frac, nil
}
