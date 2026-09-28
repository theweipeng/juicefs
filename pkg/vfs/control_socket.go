//go:build !windows

package vfs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
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
	var mu sync.Mutex
	var wg sync.WaitGroup
	connections := make(map[net.Conn]bool)
	stopped := false
	gcStop := make(chan struct{})
	wg.Add(1)
	go func() { defer wg.Done(); v.maintainChangeIndex(gcStop) }()
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
		capabilities = append(capabilities, "invalidate")
	}
	if send(changeFrame{Type: "hello", Protocol: ChangeProtocolVersion, Volume: v.Conf.ChangeVolume, Origin: v.changes.origin, Epoch: v.changes.epoch, Capabilities: capabilities}) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	queue := make(chan changeFrame, 128)
	done := make(chan struct{})
	defer close(done)
	defer func() { v.changes.Lock(); delete(v.changes.subscribers, queue); v.changes.Unlock() }()
	subscribed := false
	for scanner.Scan() {
		var request changeFrame
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			return
		}
		response := changeFrame{Type: "ack", ID: request.ID, OK: true}
		switch request.Type {
		case "subscribe":
			if subscribed {
				response.OK = false
				response.Error = "already subscribed"
				break
			}
			if err := ValidateChangePatterns(request.Patterns); err != nil {
				response.OK = false
				response.Error = err.Error()
				break
			}
			// ACK 的网络写不能持有索引锁。订阅窗口的缺口由客户端初始重同步覆盖。
			if send(response) != nil {
				return
			}
			v.changes.Lock()
			for _, pattern := range request.Patterns {
				if !slices.Contains(v.changes.patterns, pattern) {
					v.changes.patterns = append(v.changes.patterns, pattern)
				}
			}
			v.changes.subscribers[queue] = true
			v.changes.Unlock()

			subscribed = true
			go func() {
				for {
					select {
					case frame := <-queue:
						if send(frame) != nil {
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
			if err := v.InvalidatePaths(request.Changes); err != nil {
				response.OK = false
				response.Error = err.Error()
			}
		default:
			response.OK = false
			response.Error = "unsupported request"
		}
		if send(response) != nil {
			return
		}
	}
}
