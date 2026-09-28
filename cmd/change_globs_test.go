package cmd

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestChangeGlobFlag(t *testing.T) {
	patterns := []string{"**/{AGENTS,SOUL}.md", "**/ padded.md ", `**/file\*.md`}
	app := &cli.App{
		Flags: metaCacheFlags(1),
		Action: func(c *cli.Context) error {
			conf := getVfsConf(c, &meta.Config{}, &meta.Format{}, &chunk.Config{})
			require.Equal(t, patterns, conf.ChangePatterns)
			return nil
		},
	}
	require.NoError(t, app.Run([]string{"juicefs", "--change-glob", patterns[0], "--change-glob", patterns[1], "--change-glob", patterns[2]}))
}

func TestChangeGlobFlagRejectsInvalid(t *testing.T) {
	called := false
	app := &cli.App{Flags: metaCacheFlags(1), Action: func(*cli.Context) error { called = true; return nil }}
	require.Error(t, app.Run([]string{"juicefs", "--change-glob", "**/[abc"}))
	require.False(t, called)
}

func TestChangeIndexLimitFlag(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		app := &cli.App{Flags: metaCacheFlags(1)}
		require.Error(t, app.Run([]string{"juicefs", "--change-index-limit", value}))
	}
	app := &cli.App{Flags: metaCacheFlags(1), Action: func(c *cli.Context) error {
		conf := getVfsConf(c, &meta.Config{}, &meta.Format{}, &chunk.Config{})
		require.Equal(t, 8192, conf.ChangeIndexLimit)
		return nil
	}}
	require.NoError(t, app.Run([]string{"juicefs", "--change-index-limit", "8192"}))
}
