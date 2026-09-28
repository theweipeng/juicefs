package meta

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type delayedChangeAttrEngine struct {
	engine
	started chan struct{}
	resume  chan struct{}
}

func (e *delayedChangeAttrEngine) doGetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	close(e.started)
	<-e.resume
	*attr = Attr{Typ: TypeFile, Full: true, Length: 10}
	return 0
}

func TestChangeInvalidationPreventsLateMetadataFill(t *testing.T) {
	conf := DefaultConf()
	conf.OpenCache = 180 * time.Second
	m := newBaseMeta("memory", conf)
	backend := &delayedChangeAttrEngine{started: make(chan struct{}), resume: make(chan struct{})}
	m.en = backend
	const inode Ino = 100
	m.of.Open(inode, &Attr{Typ: TypeFile, Full: true})
	m.of.InvalidateChunk(inode, invalidateAllChunks)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var attr Attr
		require.Zero(t, m.GetAttr(Background(), inode, &attr))
	}()
	<-backend.started
	m.InvalidateFileCache(inode)
	close(backend.resume)
	<-done
	var cached Attr
	require.False(t, m.of.Check(inode, &cached), "失效前开始的读取不得重新填充缓存")
}
