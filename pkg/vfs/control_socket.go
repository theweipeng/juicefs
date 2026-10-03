//go:build !windows

package vfs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// StartChangeSocket 启动挂载独占的控制 socket。
func (v *VFS) StartChangeSocket() (func(), error) {
	if v.changes == nil || v.Conf.ChangeVolume == "" {
		return nil, fmt.Errorf("change volume is required")
	}
	if v.changes.index.limit <= 0 {
		return nil, fmt.Errorf("change index limit must be positive")
	}
	if err := ValidateChangePatterns(v.changes.patterns); err != nil {
		return nil, err
	}
	socket := v.Conf.ChangeSocket
	if err := os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, err
	}
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		lock.Close()
		return nil, err
	}
	if err = os.Chmod(socket, 0600); err != nil {
		listener.Close()
		lock.Close()
		return nil, err
	}
	// 控制端使用挂载配置目录的所有者，支持 root 挂载、普通业务用户连接。
	if info, err := os.Stat(filepath.Dir(socket)); err == nil {
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && os.Geteuid() == 0 {
			if err := os.Chown(socket, int(stat.Uid), int(stat.Gid)); err != nil {
				listener.Close()
				lock.Close()
				return nil, err
			}
		}
	}
	logger.Infof("file change diagnostics enabled: socket=%q volume=%q slow_after=%s watch_after=%s watch_repeat=%s", diagnosticText(socket, 2048), diagnosticText(v.Conf.ChangeVolume, 2048), invalidationSlowAfter, invalidationWatchAfter, invalidationWatchRepeat)
	var mu sync.Mutex
	var wg sync.WaitGroup
	connections := make(map[net.Conn]bool)
	stopped := false
	gcStop := make(chan struct{})
	wg.Add(1)
	go func() { defer wg.Done(); v.maintainChangeIndex(gcStop) }()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				v.logChangeHealth()
			case <-gcStop:
				return
			}
		}
	}()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if stopped {
				mu.Unlock()
				conn.Close()
				return
			}
			connections[conn] = true
			wg.Add(1)
			mu.Unlock()
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(connections, conn); mu.Unlock() }()
				v.serveChanges(conn)
			}()
		}
	}()
	return func() {
		mu.Lock()
		if stopped {
			mu.Unlock()
			return
		}
		stopped = true
		close(gcStop)
		listener.Close()
		for conn := range connections {
			conn.Close()
		}
		mu.Unlock()
		wg.Wait()
		lock.Close()
	}, nil
}

func (v *VFS) serveChanges(conn net.Conn) {
	connectionID := uuid.NewString()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var sendMu sync.Mutex
	send := func(frame changeFrame) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return json.NewEncoder(conn).Encode(frame)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if !scanner.Scan() {
		return
	}
	var hello changeFrame
	if json.Unmarshal(scanner.Bytes(), &hello) != nil || hello.Type != "hello" || hello.Protocol != ChangeProtocolVersion || hello.Volume != v.Conf.ChangeVolume {
		return
	}
	capabilities := []string{"changed"}
	if v.InvalidateEntry != nil && v.InvalidateInode != nil {
		capabilities = append(capabilities, "invalidate", "invalidate_scoped")
	}
	if send(changeFrame{Type: "hello", Protocol: ChangeProtocolVersion, Volume: v.Conf.ChangeVolume, Origin: v.changes.origin, Epoch: v.changes.epoch, Capabilities: capabilities}) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	queue := make(chan changeFrame, 128)
	done := make(chan struct{})
	defer close(done)
	unsubscribe := func() {
		v.changes.Lock()
		matcher, subscribed := v.changes.subscribers[queue]
		delete(v.changes.subscribers, queue)
		active := len(v.changes.subscribers)
		v.changes.Unlock()
		if subscribed {
			v.logChangeSubscription(connectionID, "unsubscribed", matcher, active)
		}
	}
	defer unsubscribe()
	subscribed := false
	for scanner.Scan() {
		var request changeFrame
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			return
		}
		response := changeFrame{Type: "ack", ID: request.ID, OK: true}
		var trace *invalidationTrace
		var requestErr error
		switch request.Type {
		case "subscribe":
			if subscribed {
				response.OK = false
				response.Error = "already subscribed"
				break
			}
			matcher, err := compileChangeMatcher(request.Patterns)
			if err != nil {
				response.OK = false
				response.Error = err.Error()
				break
			}
			// 完全相同的订阅无需再匹配一遍；空过滤器表示沿用配置上限。
			if sameChangePatterns(matcher, v.changes.matcher) {
				matcher = &changeMatcher{}
			}
			// ACK 的网络写不能持有索引锁。订阅窗口的缺口由客户端初始重同步覆盖。
			if send(response) != nil {
				return
			}
			v.changes.Lock()
			v.changes.subscribers[queue] = matcher
			active := len(v.changes.subscribers)
			v.changes.Unlock()
			v.logChangeSubscription(connectionID, "subscribed", matcher, active)

			subscribed = true
			go func() {
				for {
					select {
					case frame := <-queue:
						if send(frame) != nil {
							// 主请求可能仍卡在内核；发送失败时立即撤销附加范围。
							unsubscribe()
							conn.Close()
							return
						}
					case <-done:
						return
					}
				}
			}()
			continue
		case "invalidate":
			trace = v.newInvalidationTrace("control", request.ID, connectionID, request.Changes)
			if request.Patterns != nil {
				requestErr = v.invalidatePaths(request.Changes, trace, request.Patterns)
			} else {
				requestErr = v.invalidatePaths(request.Changes, trace)
			}
			if requestErr != nil {
				trace.Lock()
				recorded := trace.failureErr != nil
				trace.Unlock()
				if !recorded {
					trace.failure(requestErr)
				}
				trace.Lock()
				response.FailureStage = trace.failureStage
				trace.Unlock()
				response.OK = false
				response.Error = requestErr.Error()
				response.Errno = int(invalidationErrno(requestErr))
				if response.Errno == 0 && response.FailureStage == "validate" {
					response.Errno = int(syscall.EINVAL)
				}
			}
		default:
			response.OK = false
			response.Error = "unsupported request"
		}
		trace.phase("ack_send", 0, pathEdge{})
		sendErr := send(response)
		if trace != nil {
			if sendErr != nil {
				trace.failure(sendErr)
			}
			trace.finish(requestErr)
		}
		if sendErr != nil {
			return
		}
	}
}

func (v *VFS) logChangeSubscription(connectionID, event string, matcher *changeMatcher, active int) {
	patterns := append([]string(nil), matcher.patterns...)
	count := len(patterns)
	if len(patterns) > 20 {
		patterns = patterns[:20]
	}
	for i := range patterns {
		patterns[i] = diagnosticText(patterns[i], 2048)
	}
	encoded, _ := json.Marshal(map[string]interface{}{
		"event": event, "connection_id": connectionID, "origin": v.changes.origin, "epoch": v.changes.epoch,
		"volume": diagnosticText(v.Conf.ChangeVolume, 2048), "patterns": patterns, "pattern_count": count,
		"omitted_patterns": count - len(patterns), "configured_pattern_count": len(v.changes.patterns), "active_subscribers": active,
	})
	logger.Infof("[file-change-control] %s", encoded)
}
