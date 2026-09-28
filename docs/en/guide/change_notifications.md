# File change notifications and local cache invalidation

This fork extends JuiceFS 1.3.1 with an optional local Unix socket. A host service can subscribe to committed file changes and invalidate this mount's caches after receiving changes from other mounts. The host owns cluster discovery and transport; JuiceFS contains no Agent or Dazz path rules.

## Mount configuration

```shell
juicefs mount \
  --change-socket=/run/my-service/filesystem.sock \
  --change-volume=shared-volume \
  --change-glob='**/skills/**' \
  --change-glob='**/AGENTS.md' \
  --open-cache=180 --attr-cache=180 --entry-cache=180 --dir-entry-cache=180 \
  META-URL /mnt/shared
```

Prepare the socket parent directory for the local service account before mounting. The socket has mode 0600 and inherits that directory's owner when the mount runs as root. An advisory lock prevents two mount processes from replacing the same live socket. The mount owns and closes all control connections on exit.

Without `--change-socket`, the mount allocates no notification queue or additional path index. Other normal JuiceFS cache settings remain unchanged. The complete invalidation capability requires Linux FUSE inode and entry notifications; capability negotiation does not advertise unsupported invalidation.

## Protocol version 1

The transport is bidirectional newline-delimited UTF-8 JSON, one object per line, with a 1 MiB frame limit. Clients first negotiate the exact volume and protocol:

```json
{"type":"hello","protocol":1,"volume":"shared-volume"}
{"type":"hello","protocol":1,"volume":"shared-volume","origin":"mount-id","epoch":"boot-id","capabilities":["changed","invalidate"]}
{"type":"subscribe","id":"subscribe","patterns":["**/skills/**"]}
{"type":"ack","id":"subscribe","ok":true}
```

Patterns use [doublestar v4](https://github.com/bmatcuk/doublestar) path matching. `*` and `?` do not cross `/`; a `**` path component spans zero or more directories. Character groups (`[abc]`, `[a-z]`, `[!abc]`), alternatives (`{AGENTS,SOUL}.md`) and backslash escapes are supported. Matching is case-sensitive and includes dotfiles and Unicode names. An optional leading `/` refers to the volume root. Only path strings are matched: this does not invoke `Glob` or traverse the filesystem.

Pass each pattern as a separate, quoted `--change-glob` argument. Commas and whitespace inside an argument are preserved; for example, `--change-glob='**/{AGENTS,SOUL}.md'`. Patterns must be valid UTF-8, nonempty, contain no NUL, and be at most 4096 bytes. Mount flags and each subscription accept at most 256 patterns. Invalid flags fail before mounting; invalid subscriptions receive `ok:false` and add no patterns.

Subscription patterns are added to the mount's configured interest set. They continue matching newly created directories. Clients must perform conservative revalidation after connecting: subscription setup and disconnected intervals are not replayed. Recursive directory events are reported conservatively, including ancestors that do not directly match a pattern.

```json
{"type":"changed","origin":"mount-id","epoch":"boot-id","sequence":42,"changes":[{"path":"/agents/a/skills/new","old_path":"/agents/a/skills/old","operation":"renamed","recursive":true}]}
{"type":"invalidate","id":"invalidate-1","changes":[{"path":"/agents/a/skills","recursive":true}]}
{"type":"ack","id":"invalidate-1","ok":true}
```

Paths are canonical absolute paths **within the volume**, independent of the mount point. Relative paths, traversal components, duplicate separators and NUL are rejected. Invalidation accepts at most 4096 requested changes. A rename reports both old and new paths, including moves out of the interest set. Directory moves are conservatively recursive.

An ACK only describes the local request. Failed or unsupported invalidation returns `ok:false` and `error`; it must not be treated as a cluster synchronization barrier. Duplicate invalidations are safe. Invalidation does not emit new change events.

Notifications carry an instance origin, boot epoch and monotonically increasing source sequence. They are not file versions and provide no cross-writer order. A full subscriber queue is drained into `resync_required` with `resync:true`; oversized changes similarly become conservative resynchronization. Clients detect sequence gaps and epoch changes and invalidate a suitable conservative range. There is no durable event log.

## Commit and cache behavior

Buffered `write()` does not emit a committed content change. The writer emits only after object data is available and the corresponding metadata write succeeds. Creation, truncation, allocation/copy, links, unlink, directory operations and rename emit after their metadata operation succeeds. A long-running stream can therefore generate multiple committed changes; the host decides its batching policy.

The local path index is populated by lookup, readdir, create, link and rename. Current relationships and retired relationships are tracked separately, including negative entries. Retired relationships are reclaimed after successful local inode and entry invalidation. An idle entry becomes eligible after five minutes; capacity pressure starts reclaiming cold entries at 75% occupancy. Active handles retain their positioning records, and directory ancestors remain while indexed children need them. Successful explicit invalidation also releases the retired relationships it covered.

Reclamation runs in a mount-owned background task, independent of the host's connection. Each pass handles at most 128 records, releases the index lock before kernel calls, and verifies the observation generation before deleting a record. Kernel failures retain records for retry. Closing the notification socket service stops and joins its maintenance task. No write callback calls `GetPaths()` or scans a shared directory.

`--change-index-limit` bounds the total current and historical relation count (default 262144, must be positive). If successful reclamation cannot keep up and the hard limit is reached, ordinary file operations continue, further indexing is bounded, notifications request resynchronization, and `invalidate` returns `ENOBUFS` through a failed ACK. This degraded state requires remounting, with a suitably sized limit, to restore reliable invalidation: freed space alone cannot prove that unrecorded kernel caches were cleared. Monitor occupancy, retired records and reclamation failures before reaching the limit.

Path expansion has separate depth, work, byte and result budgets. Missing parents, incomplete hard-link coverage, cycles or an exhausted budget explicitly mark the result incomplete and trigger conservative resynchronization. A partial path list is never treated as a complete list.

`invalidate` clears matching open-cache metadata and chunk mappings, symlink/parent caches, VFS read buffers, directory handles, and FUSE inode/entry caches. New entries invalidate known parents and negative entries. Recursive invalidation walks only the local index and locally cached/open inode sets. Immutable object block data remains reusable.

Metadata cache fills compare an invalidation epoch so a read started before invalidation cannot restore stale cached metadata. Existing in-flight operations may finish with their old result. Kernel invalidation runs in the control connection's goroutine without holding file operation locks. Targets with writable handles return EBUSY conservatively, protecting uncommitted data. Kernel errors propagate to the ACK.

This mechanism affects later reads. It does not rewrite application buffers, switch existing file handles to a new execution snapshot, serialize concurrent writers, or make multiple file writes atomic.

## Metrics and tests

Additional Prometheus metrics, registered only when notifications are configured:

- `change_notification_path_edges`
- `change_notification_path_records`, `change_notification_path_retired`, `change_notification_path_limit`
- `change_notification_path_reclaimed_total`, `change_notification_reclaim_failures_total`
- `change_notification_index_overflows_total`, `change_notification_index_incomplete`
- `change_notifications_total`
- `change_notification_overflows_total`
- `cache_invalidation_requests_total`
- `cache_invalidation_failures_total`

Use the existing JuiceFS process, metadata request and cache metrics to evaluate CPU, memory and metadata overhead.

```shell
go test -race ./pkg/meta ./pkg/vfs ./pkg/fuse ./cmd \
  -run 'TestChange|TestCommitted|TestInvalidation' -count=1
# Linux, /dev/fuse and mount privileges required:
sudo python3 tests/file_change_notifications.py ./juicefs-linux-amd64
```

The Linux test uses two mounts of one metadata volume, warms 180-second caches, and verifies replacement, cached absence, creation, deletion, ZIP extraction, truncation, hard links and directory moves after explicit invalidation. It also checks 200 consecutive atomic replacements, fresh reads on the other mount, and index reclamation after the source subscriber disconnects. The release workflow runs it before uploading the binary and SHA-256 checksum. Existing OceanBase SQL, OSS V1 signature and CGO/glibc 2.17 build patches remain part of the release.
