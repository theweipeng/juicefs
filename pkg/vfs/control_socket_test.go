//go:build !windows

package vfs

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestChangeSocketScopedRootAndExplicitInvalidationKeepSeparateScopes(t *testing.T) {
	v := changeGCFixture(t)
	v.changes = newChangeNotifier([]string{"skill/**"})
	v.Conf.ChangeVolume = "test"
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	outside, handle, st := v.Create(ctx, rootID, "outside", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	t.Cleanup(func() { v.Release(ctx, outside.Inode, handle) })
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
	t.Cleanup(func() { client.Close(); <-done })
	require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
	encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
	var frame changeFrame
	require.NoError(t, encoder.Encode(changeFrame{Type: "hello", Protocol: 1, Volume: "test"}))
	require.NoError(t, decoder.Decode(&frame))
	require.Contains(t, frame.Capabilities, "invalidate_scoped")
	// 裁剪后的根请求不能因配置外的写句柄失败。
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "scoped-root", Patterns: []string{"skill/**"}, Changes: []FileChange{{Path: "/", Recursive: true}}}))
	require.NoError(t, decoder.Decode(&frame))
	require.True(t, frame.OK)
	// 主动 API 不受通知 glob 限制，配置外目标仍执行可写句柄保护。
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "explicit-outside", Changes: []FileChange{{Path: "/outside"}}}))
	require.NoError(t, decoder.Decode(&frame))
	require.False(t, frame.OK)
	require.Equal(t, int(syscall.EBUSY), frame.Errno)
	require.Equal(t, "busy_handles", frame.FailureStage)
	require.NoError(t, encoder.Encode(changeFrame{Type: "invalidate", ID: "explicit-root", Changes: []FileChange{{Path: "/", Recursive: true}}}))
	require.NoError(t, decoder.Decode(&frame))
	require.False(t, frame.OK)
	require.Equal(t, int(syscall.EBUSY), frame.Errno)
}

func TestChangeSocketProtocolAndRename(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**/SKILL.md", "**/{SOUL,报告}.md"})
	v.Conf.ChangeVolume = "test"
	v.InvalidateInode = func(Ino) syscall.Errno { return 0 }
	v.InvalidateEntry = func(Ino, string) syscall.Errno { return 0 }
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); defer server.Close(); v.serveChanges(server) }()
	defer func() { client.Close(); <-done }()
	require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
	encoder, decoder := json.NewEncoder(client), json.NewDecoder(client)
	send := func(frame changeFrame) { require.NoError(t, encoder.Encode(frame)) }
	receive := func() changeFrame { var frame changeFrame; require.NoError(t, decoder.Decode(&frame)); return frame }
	send(changeFrame{Type: "hello", Protocol: 1, Volume: "test"})
	hello := receive()
	require.Contains(t, hello.Capabilities, "invalidate")
	require.NotEmpty(t, hello.Origin)
	// 无效模式不能收到成功 ACK，也不能部分加入关注范围。
	send(changeFrame{Type: "subscribe", ID: "bad-glob", Patterns: []string{"**/should-not-watch", "**/[abc"}})
	rejected := receive()
	require.False(t, rejected.OK)
	require.Equal(t, "bad-glob", rejected.ID)
	require.Equal(t, []string{"**/SKILL.md", "**/{SOUL,报告}.md"}, v.changes.patterns)
	send(changeFrame{Type: "subscribe", ID: "sub", Patterns: []string{"**/SKILL.md", "**/{SOUL,报告}.md"}})
	require.True(t, receive().OK)
	// 失效请求也作为订阅完成的同步点，不依赖 goroutine 调度时机。
	send(changeFrame{Type: "invalidate", ID: "initial", Changes: []FileChange{{Path: "/", Recursive: true}}})
	require.True(t, receive().OK)
	v.notifyEntry(rootID, "报告.md", 41, "created", false)
	require.Equal(t, "/报告.md", receive().Changes[0].Path)
	v.notifyRename(rootID, "SKILL.md", rootID, "away.md", 42, false, 0)
	changed := receive()
	require.Equal(t, "changed", changed.Type)
	require.Equal(t, "/SKILL.md", changed.Changes[0].OldPath)
	require.Equal(t, "/away.md", changed.Changes[0].Path)
	require.Equal(t, hello.Origin, changed.Origin)
	send(changeFrame{Type: "invalidate", ID: "invalid", Changes: []FileChange{{Path: "/../bad"}}})
	response := receive()
	require.Equal(t, "invalid", response.ID)
	require.False(t, response.OK)
	// 移入关注范围也必须报告；未命中的普通文件重命名不制造事件。
	v.notifyRename(rootID, "other.md", rootID, "SKILL.md", 43, false, 0)
	require.Equal(t, "/SKILL.md", receive().Changes[0].Path)
	v.changes.Lock()
	before := v.changes.sequence
	v.changes.Unlock()
	v.notifyRename(rootID, "log-1", rootID, "log-2", 44, false, 0)
	v.changes.Lock()
	require.Equal(t, before, v.changes.sequence)
	v.changes.Unlock()
}

func TestChangeSocketRejectsInvalidPatternsBeforeListen(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.changes = newChangeNotifier([]string{"**/[abc"})
	v.Conf.ChangeVolume = "test"
	v.Conf.ChangeSocket = filepath.Join(t.TempDir(), "changes.sock")
	stop, err := v.StartChangeSocket()
	require.Error(t, err)
	require.Nil(t, stop)
	require.NoFileExists(t, v.Conf.ChangeSocket)
}

func TestChangeSocketOwnsMaintenanceWithoutSubscribers(t *testing.T) {
	v := changeGCFixture(t)
	v.Conf.ChangeVolume = "test"
	// macOS 临时目录加测试名会超过 Unix socket 路径长度限制。
	directory, err := os.MkdirTemp("/tmp", "jfs-change-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	v.Conf.ChangeSocket = filepath.Join(directory, "changes.sock")
	stop, err := v.StartChangeSocket()
	require.NoError(t, err)
	t.Cleanup(stop)
	v.changes.remember(rootID, "config", 10)
	v.changes.remember(rootID, "config", 11)
	require.Eventually(t, func() bool {
		v.changes.Lock()
		defer v.changes.Unlock()
		return v.changes.index.size() == 1
	}, 3*time.Second, time.Millisecond)
	stop()
	stop()
	// stop 已等待后台任务退出，退出后的关系不会再被异步访问。
	v.changes.remember(rootID, "config", 12)
	v.changes.Lock()
	require.Equal(t, 2, v.changes.index.size())
	v.changes.Unlock()
}
