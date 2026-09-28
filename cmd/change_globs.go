package cmd

import (
	"strings"

	"github.com/juicedata/juicefs/pkg/vfs"
)

// cli.StringSlice 会按逗号拆分并去掉空白；glob 必须完整保留每次参数。
type changeGlobs []string

func (g *changeGlobs) Set(value string) error {
	if err := vfs.ValidateChangePatterns(append([]string(*g), value)); err != nil {
		return err
	}
	*g = append(*g, value)
	return nil
}

func (g *changeGlobs) String() string {
	return strings.Join(*g, ", ")
}
