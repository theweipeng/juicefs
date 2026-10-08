package meta

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type directoryParentEngine struct {
	engine
	attrs   map[Ino]Attr
	entries []*Entry
	limit   int
	onRead  func()
}

func (e *directoryParentEngine) doGetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	value, ok := e.attrs[inode]
	if !ok {
		return syscall.ENOENT
	}
	*attr = value
	return 0
}

func (e *directoryParentEngine) doReaddir(ctx Context, inode Ino, plus uint8, entries *[]*Entry, limit int) syscall.Errno {
	e.limit = limit
	if e.onRead != nil {
		e.onRead()
	}
	end := len(e.entries)
	if end > limit {
		end = limit
	}
	*entries = append(*entries, e.entries[:end]...)
	return 0
}

func TestDirectoryParentBoundedFallbackAndSubdirRoot(t *testing.T) {
	m := newBaseMeta("memory", DefaultConf())
	m.root = 10
	e := &directoryParentEngine{attrs: map[Ino]Attr{20: {Typ: TypeDirectory, Parent: 10}}, entries: []*Entry{{Inode: 20, Name: []byte("skill"), Attr: &Attr{Typ: TypeDirectory}}}}
	m.en = e
	parent, name, st := m.GetDirParent(Background(), 20)
	require.Zero(t, st)
	require.Equal(t, RootInode, parent)
	require.Equal(t, "skill", name)
	require.Equal(t, 256, e.limit)
	e.attrs[20] = Attr{Typ: TypeDirectory, Parent: RootInode}
	_, _, st = m.GetDirParent(Background(), 20)
	require.Equal(t, syscall.EAGAIN, st, "挂载子目录之外的真实根不能冒充逻辑根")
	e.attrs[20] = Attr{Typ: TypeDirectory, Parent: 10}
	for i := 0; i < 256; i++ {
		e.entries = append([]*Entry{{Inode: 1000 + Ino(i), Name: []byte("noise"), Attr: &Attr{Typ: TypeDirectory}}}, e.entries...)
	}
	_, _, st = m.GetDirParent(Background(), 20)
	require.Equal(t, syscall.EAGAIN, st)
}

func TestDirectoryParentRejectsMoveAndInvalidNames(t *testing.T) {
	for _, name := range []string{"skill", "..", "bad/name", "bad\x00name"} {
		t.Run(name, func(t *testing.T) {
			m := newBaseMeta("memory", DefaultConf())
			e := &directoryParentEngine{attrs: map[Ino]Attr{20: {Typ: TypeDirectory, Parent: RootInode}}, entries: []*Entry{{Inode: 20, Name: []byte(name), Attr: &Attr{Typ: TypeDirectory}}}}
			if name == "skill" {
				e.onRead = func() { e.attrs[20] = Attr{Typ: TypeDirectory, Parent: 30} }
			}
			m.en = e
			_, _, st := m.GetDirParent(Background(), 20)
			require.Equal(t, syscall.EAGAIN, st)
		})
	}
}
