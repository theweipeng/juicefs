package vfs

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/bmatcuk/doublestar/v4"
)

type changeMatcher struct {
	patterns  []string
	ancestors []string
}

// compileChangeMatcher 仅编译字符串，目录祖先匹配不依赖共享目录扫描。
func compileChangeMatcher(patterns []string) (*changeMatcher, error) {
	if len(patterns) > 256 {
		return nil, fmt.Errorf("too many change patterns (maximum 256)")
	}
	matcher := &changeMatcher{}
	patternsSeen := make(map[string]bool)
	seen := make(map[string]bool)
	bytes := 0
	addPrefix := func(prefix string) error {
		if prefix == "" || seen[prefix] {
			return nil
		}
		bytes += len(prefix)
		if len(seen) >= 8192 || bytes > 8*1024*1024 {
			return fmt.Errorf("change glob ancestors exceed compilation budget")
		}
		seen[prefix] = true
		matcher.ancestors = append(matcher.ancestors, prefix)
		return nil
	}
	for _, value := range patterns {
		if value == "" || len(value) > 4096 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) || !doublestar.ValidatePattern(value) {
			return nil, fmt.Errorf("invalid change glob %q", value)
		}
		pattern := strings.TrimPrefix(value, "/")
		if patternsSeen[pattern] {
			continue
		}
		patternsSeen[pattern] = true
		matcher.patterns = append(matcher.patterns, pattern)
		depth, class := 0, false
		classStart := 0
		for i := 0; i < len(pattern); i++ {
			c := pattern[i]
			end := i
			if c == '\\' {
				i++
				if i >= len(pattern) || pattern[i] != '/' {
					continue
				}
				c = '/'
			} else if c == '[' && !class {
				class = true
				classStart = i
			} else if c == ']' {
				class = false
				if doublestar.MatchUnvalidated(pattern[classStart:i+1], "/") {
					if err := addPrefix(pattern[:classStart] + strings.Repeat(",}", depth)); err != nil {
						return nil, err
					}
				}
			}
			if class {
				continue
			}
			switch c {
			case '{':
				depth++
			case '}':
				depth--
			case '/':
				if end == 0 {
					continue
				}
				// 补齐尚未闭合的备选组，保留跨目录与嵌套备选的祖先。
				prefix := pattern[:end] + strings.Repeat(",}", depth)
				if err := addPrefix(prefix); err != nil {
					return nil, err
				}
			}
		}
	}
	return matcher, nil
}

func (m *changeMatcher) matches(value string, recursive bool) bool {
	if m == nil {
		return false
	}
	value = strings.TrimPrefix(value, "/")
	for _, pattern := range m.patterns {
		if doublestar.MatchUnvalidated(pattern, value) {
			return true
		}
	}
	if recursive {
		if value == "" {
			return len(m.patterns) > 0
		}
		for _, prefix := range m.ancestors {
			if doublestar.MatchUnvalidated(prefix, value) {
				return true
			}
		}
	}
	return false
}

func (m *changeMatcher) matchesChange(change FileChange) bool {
	return m.matches(change.Path, change.Recursive) || change.OldPath != "" && m.matches(change.OldPath, change.Recursive)
}

func sameChangePatterns(a, b *changeMatcher) bool {
	if len(a.patterns) != len(b.patterns) {
		return false
	}
	seen := make(map[string]bool, len(a.patterns))
	for _, pattern := range a.patterns {
		seen[pattern] = true
	}
	for _, pattern := range b.patterns {
		if !seen[pattern] {
			return false
		}
	}
	return true
}
