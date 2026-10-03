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
{"type":"hello","protocol":1,"volume":"shared-volume","origin":"mount-id","epoch":"boot-id","capabilities":["changed","invalidate","invalidate_scoped"]}
{"type":"subscribe","id":"subscribe","patterns":["**/skills/**"]}
{"type":"ack","id":"subscribe","ok":true}
```

Patterns use [doublestar v4](https://github.com/bmatcuk/doublestar) path matching. `*` and `?` do not cross `/`; a `**` path component spans zero or more directories. Character groups (`[abc]`, `[a-z]`, `[!abc]`), alternatives (`{AGENTS,SOUL}.md`) and backslash escapes are supported. Matching is case-sensitive and includes dotfiles and Unicode names. An optional leading `/` refers to the volume root. Only path strings are matched: this does not invoke `Glob` or traverse the filesystem.

Pass each pattern as a separate, quoted `--change-glob` argument. Commas and whitespace inside an argument are preserved; for example, `--change-glob='**/{AGENTS,SOUL}.md'`. Patterns must be valid UTF-8, nonempty, contain no NUL, and be at most 4096 bytes. Mount flags and each subscription accept at most 256 patterns. Invalid flags fail before mounting; invalid subscriptions receive `ok:false` and add no patterns.

Configured mount patterns bound all automatic notifications. Subscription filters can only narrow that set, never extend it. Identical mount and subscription patterns are evaluated once. Recursive events must intersect configured interests; ancestor deletion and moves remain visible. Pattern ancestor compilation is bounded to 8192 prefixes and 8 MiB; excessive configurations are rejected.

Clients must perform conservative revalidation after connecting: subscription setup and disconnected intervals are not replayed. A connection whose filters omit a globally interesting change receives an empty `changed` progress frame with the same source sequence. Consume its sequence without producing a file change. This preserves gap detection without exposing another subscription's paths.

```json
{"type":"changed","origin":"mount-id","epoch":"boot-id","sequence":42,"changes":[{"path":"/agents/a/skills/new","old_path":"/agents/a/skills/old","operation":"renamed","recursive":true}]}
{"type":"invalidate","id":"invalidate-1","changes":[{"path":"/agents/a/skills","recursive":true}]}
{"type":"ack","id":"invalidate-1","ok":true}
```

Paths are canonical absolute paths **within the volume**, independent of the mount point. Relative paths, traversal components, duplicate separators and NUL are rejected. Invalidation accepts at most 4096 requested changes. A rename reports both old and new paths, including moves out of the interest set. Directory moves are conservatively recursive.

Two-file exchange is not recursive. An exchange involving a directory is recursive; both inode identities and types come from the same metadata operation. Explicit invalidation is independent of notification filters and may target any valid volume path.

The `invalidate_scoped` capability accepts an optional `patterns` array on an invalidation request. It invalidates only the intersection of those patterns and requested paths, including required ancestor entries and negative caches. Unmatched subtrees are not traversed or evicted. An empty array matches nothing. Omit the field for ordinary explicit invalidation. A scoped ACK does not prove that an entire recursive input range was cleared.

An ACK only describes the local request. Failed invalidation returns `ok:false`, `error`, numeric `errno` when available, and `failure_stage`. These optional fields do not change protocol version 1. An ACK must not be treated as a cluster synchronization barrier. Duplicate invalidations are safe. Invalidation does not emit new change events.

```json
{"type":"ack","id":"invalidate-1","ok":false,"error":"device or resource busy","errno":16,"failure_stage":"busy_handles"}
```

Notifications carry an instance origin, boot epoch and monotonically increasing source sequence. They are not file versions and provide no cross-writer order. A full subscriber queue is drained into `resync_required` with `resync:true`; oversized changes similarly become conservative resynchronization. Clients detect sequence gaps and epoch changes and invalidate a suitable conservative range. There is no durable event log.

## Commit and cache behavior

Buffered `write()` does not emit a committed content change. The writer emits only after object data is available and the corresponding metadata write succeeds. Creation, truncation, allocation/copy, links, unlink, directory operations and rename emit after their metadata operation succeeds. A long-running stream can therefore generate multiple committed changes; the host decides its batching policy.

The local path index is populated by lookup, readdir, create, link and rename. Current relationships and retired relationships are tracked separately, including negative entries. Retired relationships are reclaimed after successful local inode and entry invalidation. An idle entry becomes eligible after five minutes; capacity pressure starts reclaiming cold entries at 75% occupancy. Active handles retain their positioning records, and directory ancestors remain while indexed children need them. Successful explicit invalidation also releases the retired relationships it covered.

Reclamation runs in a mount-owned background task, independent of the host's connection. Each pass handles at most 128 records, releases the index lock before kernel calls, and verifies the observation generation before deleting a record. Kernel failures retain records for retry. Closing the notification socket service stops and joins its maintenance task. No write callback calls `GetPaths()` or scans a shared directory.

`--change-index-limit` bounds the total current and historical relation count (default 262144, must be positive). If successful reclamation cannot keep up and the hard limit is reached, ordinary file operations continue, further indexing is bounded, notifications request resynchronization, and `invalidate` returns `ENOBUFS` through a failed ACK. This degraded state requires remounting, with a suitably sized limit, to restore reliable invalidation: freed space alone cannot prove that unrecorded kernel caches were cleared. Monitor occupancy, retired records and reclamation failures before reaching the limit.

Path expansion has separate depth, work, byte and result budgets. Missing parents, incomplete hard-link coverage, cycles or an exhausted budget explicitly mark the result incomplete and trigger conservative resynchronization. A partial path list is never treated as a complete list.

A proven path match may identify its physical inode even when additional aliases are unknown. An unmatched entry with incomplete parent paths cannot be silently included in a targeted invalidation. The entire request returns EAGAIN with `failure_stage=index_scope` before changing caches, with the unresolved parent and known path samples in diagnostics. Retry after local positioning is restored. An explicitly requested recursive volume root covers unpositioned local targets, but a permanently incomplete capacity-exhausted index still returns ENOBUFS. There is no automatic root fallback.

Only parent chains affected by gaps or directory-history changes require position verification. Already positioned unrelated directories are not revisited. `position_checked_parents` records this bounded verification work; `position_check_parents` in health summaries reports pending parent checks. An unresolved subtree is checked before any cache mutation, not replaced by a volume-wide clear.

`invalidate` clears matching open-cache metadata and chunk mappings, symlink/parent caches, VFS read buffers, directory handles, and FUSE inode/entry caches. New entries invalidate known parents and negative entries. Recursive invalidation walks the selected local index subtrees. Only an unscoped, explicitly recursive `/` request supplements them with all cached and open inodes. A scoped root request never performs that supplement. Immutable object block data remains reusable.

Required ancestor metadata is refreshed without recursively evicting sibling contents. Hard links share one physical inode, so a writable handle opened through another alias can legitimately return EBUSY. Renames and exchanges retain relevant historical identities until their old cache relationships are cleared. These are not permission to include unrelated inodes. Background `source=reclaim` maintenance independently invalidates bounded cold or retired relationships, including entries outside notification patterns; distinguish its cache cost from request scope.

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
- `change_notification_subscribers`, `change_notification_subscription_patterns`
- `cache_invalidation_requests_total`
- `cache_invalidation_failures_total`
- `cache_invalidation_duration_seconds{source}` (histogram)
- `cache_invalidation_stage_duration_seconds{source,stage}` (histogram)
- `cache_invalidation_results_total{source,result}`
- `cache_invalidation_stage_failures_total{source,stage,errno}`
- `cache_invalidation_in_flight{source}`
- `cache_invalidation_oldest_in_flight_age_seconds{source}`
- `cache_invalidation_lock_held_seconds`

The mount exports these names with the `juicefs_` prefix and its existing mount labels. Sources are `control`, `direct`, and `reclaim`. Results are `ok`, `busy`, `capacity`, `unsupported`, `invalid`, `scope_uncertain`, `kernel_error`, `ack_error`, and `other`. Paths, request IDs, inode IDs and PID values are never metric labels. Stage failures count failed targets; results count completed operations. In-flight ages expose requests that have not completed yet. Subscription-pattern counts include duplicates across active connections and exclude configured patterns.

Use the Prometheus `/metrics` endpoint for labeled analysis. The `.stats` text format does not preserve all labels. Configure scraping using the address in `Prometheus metrics listening on ...`, or explicitly set `--metrics=127.0.0.1:9567`. Keep access restricted to trusted monitoring services. Use the existing process, metadata request and cache metrics to evaluate CPU, memory and metadata overhead.

Import [the example alert rules](change_notification_alerts.yml) into your monitoring configuration and tune thresholds for the workload. They are not installed automatically.

## Locate a failed or slow invalidation

Mount diagnostics use the `[cache-invalidation]` marker followed by single-line JSON. Collect the mount process's logs in addition to the host service's logs. Foreground mounts write to stderr; background mounts use the configured `--log` file. Default INFO logging includes warnings; an ERROR-only configuration hides slow-request warnings. Enabling DEBUG also records fast successful completions.

Subscription lifecycle INFO logs use `[file-change-control]`, recording connection identity, bounded additional pattern samples, configured-pattern count and active subscriber count. Use them to identify an active broad subscription or verify that its disconnect removed the extra scope.

The same marker emits a `health_summary` every minute: configured patterns, input/filtered/matched counts, matching cost, index pressure, unfinished requests and cumulative invalidation/stage durations. Counters use `counter_scope=since_mount_start`; compare snapshots within one origin/epoch for window totals. A busy index does not block this diagnostic. Matching counters are `change_notification_input_changes_total`, `change_notification_filtered_changes_total`, `change_notification_matched_changes_total`, and `change_notification_matching_seconds_total`.

Correlate the host's socket `request_id` with the mount's `request_id`, volume, time and mount origin/epoch. Each connection has a separate `connection_id`. `request_digest` hashes the mount's complete sorted input changes; it is not the host's `changes_digest`. Logged input paths are limited to 20 entries and 2048 bytes per entry, with omitted/truncated counts. Arbitrary long request IDs are replaced with a SHA-256 identifier. JSON escaping prevents path strings from injecting log lines. No file contents or metadata connection credentials are logged.

Failed requests and completions taking at least one second emit WARNING. A watchdog emits `event=blocked` after two seconds, then every 30 seconds until completion. It uses independent diagnostic state, so it can report while invalidation or index locks are held. `event=finished` includes the result and accumulated time per stage. A blocked event is a diagnostic warning, not cancellation or a failed ACK. Client-side timeout does not interrupt the mount operation.

| Stage | What to inspect |
| --- | --- |
| `invalidation_lock` | The request is waiting for the shared invalidation lock. `lock_owner` identifies the operation holding it, including its source, stage, targets and hold time. Reclamation shares this lock. |
| `index_lock`, `index_retire_lock` | The request is waiting for the local path-index lock. |
| `index_match` | Targeted directory-entry lookup. Compare `scanned_edges` and matched targets. Only affected subtrees are expanded. `position_checked_parents` records verification of affected parent chains after gaps or directory-history changes, not a scan of every positioned parent. No shared directory is scanned. |
| `index_scope` | Targeting could not be proved. Inspect `unresolved_parent_inode`, `known_parent_paths` and `scope_reason`; EAGAIN does not mean a writable handle was encountered. |
| `cached_inode_collect` | Collecting locally cached/open inode targets for a root resynchronization. |
| `busy_handles`, `busy_path_lookup` | Checking active handles or resolving a rejected inode's local paths. `writable_handle` protects an open writer. `active_handle` is an expected reclamation retention condition. |
| `metadata_cache`, `read_buffers`, `attributes`, `directory_handles` | Clearing mount-local caches or waiting for their internal locks. |
| `kernel_inode`, `kernel_entry` | Kernel notification. Target inode or parent/name and error samples identify the failing operation. Kernel EBUSY is distinct from writable-handle protection. |
| `index_candidates`, `index_retire` | Selecting or retiring local index records. |
| `ack_send` | Invalidation processing has returned; the control response is waiting on the connection send lock or socket write. |

For writable-handle rejection, inspect `busy_inode`, `busy_fh`, `busy_flags`, `busy_opener_pid`, `busy_opener_uid`, `busy_open_age_ms`, and `busy_paths`. PID identifies the process that opened the handle, not necessarily a later writer using an inherited/shared descriptor. PID zero means the opener is unknown. Paths may be incomplete or historical; check `busy_paths_complete`. Enrichment uses nonblocking lock attempts: `busy_handle_metadata_unavailable` or `busy_paths_unavailable` means that detail was unavailable without delaying rejection. Use `ps` and `lsof -p PID` on the affected host when the process is still alive.

Fast reclamation deferred by active handles stays at DEBUG, with failure metrics still incremented. It must not hide kernel errors in the same reclamation pass. A pass records at most eight failure samples. The final error still propagates through the normal ACK contract; logging does not relax cache safety or writable-handle protection.

Before switching binaries, verify that the log collector indexes `request_id`, `stage`, `failure_stage`, `errno` and `busy_opener_pid`. Inject one rejected invalidation and one delayed request; check logs and metrics from the actual mount. Unit tests cannot validate production collection, scrape configuration, process termination before logging, or Linux FUSE behavior.

```shell
go test -race ./pkg/meta ./pkg/vfs ./pkg/fuse ./cmd \
  -run 'TestChange|TestCommitted|TestInvalidation' -count=1
# Linux, /dev/fuse and mount privileges required:
sudo python3 tests/file_change_notifications.py ./juicefs-linux-amd64
```

The Linux test uses two mounts of one metadata volume, warms 180-second caches, and verifies replacement, cached absence, creation, deletion, ZIP extraction, truncation, hard links and directory moves after explicit invalidation. It also checks 200 consecutive atomic replacements, fresh reads on the other mount, and index reclamation after the source subscriber disconnects. The release workflow runs it before uploading the binary and SHA-256 checksum. Existing OceanBase SQL, OSS V1 signature and CGO/glibc 2.17 build patches remain part of the release.
