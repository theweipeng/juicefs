#!/usr/bin/env python3
"""Linux 多挂载的真实缓存失效验收。"""

import argparse
import errno
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import zipfile


def connect(path):
    connection = socket.socket(socket.AF_UNIX)
    connection.settimeout(10)
    connection.connect(str(path))
    stream = connection.makefile("rwb", buffering=0)
    def send(value):
        stream.write(json.dumps(value).encode() + b"\n")
    def receive():
        return json.loads(stream.readline())
    send({"type": "hello", "protocol": 1, "volume": "test"})
    assert receive()["protocol"] == 1
    send({"type": "subscribe", "id": "subscribe", "patterns": ["skill/**", "moved/**"]})
    assert receive()["ok"]
    return connection, send, receive


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = str(args.binary.resolve())
    with tempfile.TemporaryDirectory(prefix="jfs-change-", dir="/tmp") as directory:
        root = Path(directory)
        metadata = "sqlite3://" + str(root / "meta.db")
        subprocess.run([binary, "format", "--storage", "file", "--bucket", str(root / "objects") + "/", metadata, "test"], check=True)
        mounts = [root / "a", root / "b"]
        processes = []
        try:
            for name, mount in zip(("a", "b"), mounts):
                mount.mkdir()
                with (root / (name + ".log")).open("wb") as log:
                    process = subprocess.Popen([binary, "mount", "--cache-dir", str(root / (name + "-cache")), "--attr-cache=180", "--entry-cache=180", "--dir-entry-cache=180", "--negative-entry-cache=180", "--open-cache=180", "--change-socket", str(root / (name + ".sock")), "--change-volume=test", "--change-glob=skill/**", "--change-glob=moved/**", "--backup-meta=0", "--no-usage-report", metadata, str(mount)], stdout=log, stderr=subprocess.STDOUT)
                processes.append(process)
            deadline = time.monotonic() + 30
            while not all(os.path.ismount(mount) for mount in mounts):
                if time.monotonic() > deadline or any(process.poll() is not None for process in processes):
                    raise RuntimeError("挂载失败")
                time.sleep(0.1)
            a, b = mounts
            connection_a, _, receive_a = connect(root / "a.sock")
            connection_b, send_b, receive_b = connect(root / "b.sock")
            def notification_count(mount):
                return next(
                    float(line.split()[1]) for line in (mount / ".stats").read_text().splitlines()
                    if line.startswith("juicefs_change_notifications_total ")
                )
            before_noise = notification_count(a)
            (a / "_runtime/llm_snapshots/run").mkdir(parents=True)
            (a / "_runtime/llm_snapshots/run/output.txt").write_text("not subscribed")
            assert notification_count(a) == before_noise
            request_sequence = 0
            def request_invalidation(path, recursive=False, patterns=None):
                nonlocal request_sequence
                request_sequence += 1
                request_id = f"invalidate-{request_sequence}"
                request = {"type": "invalidate", "id": request_id, "changes": [{"path": path, "recursive": recursive}]}
                if patterns is not None:
                    request["patterns"] = patterns
                send_b(request)
                while True:
                    response = receive_b()
                    if response.get("id") == request_id:
                        return response
            def invalidate(path, recursive=False):
                response = request_invalidation(path, recursive)
                assert response["ok"], response
            response = request_invalidation("/../invalid")
            assert not response["ok"] and response["errno"] == errno.EINVAL, response
            assert response["failure_stage"] == "validate", response
            # fsync 后保持可写句柄，真实挂载必须指出具体占用及打开者。
            with (b / "busy.txt").open("w") as stream:
                stream.write("committed but still open")
                stream.flush()
                os.fsync(stream.fileno())
                response = request_invalidation("/busy.txt")
                assert not response["ok"] and response["errno"] == errno.EBUSY, response
                assert response["failure_stage"] == "busy_handles", response
            deadline = time.monotonic() + 5
            while True:
                records = [
                    json.JSONDecoder().raw_decode(line.split("[cache-invalidation] ", 1)[1])[0]
                    for line in (root / "b.log").read_text().splitlines()
                    if "[cache-invalidation] " in line
                ]
                matches = [record for record in records if record.get("request_id") == response["id"] and record.get("event") == "finished"]
                if matches:
                    record = matches[-1]
                    assert record["busy_opener_pid"] == os.getpid(), record
                    assert record["busy_paths"] == ["/busy.txt"], record
                    assert record["busy_inode"] > 0 and record["busy_fh"] >= 0, record
                    break
                assert time.monotonic() < deadline, records
                time.sleep(0.05)
            invalidate("/busy.txt")
            assert "juicefs_cache_invalidation_stage_failures_total" in (b / ".stats").read_text()
            (a / "skill").mkdir()
            (a / "skill/SKILL.md").write_text("old")
            assert (b / "skill/SKILL.md").read_text() == "old"
            assert not (b / "skill/new.txt").exists()
            list((b / "skill").iterdir())
            (a / "replacement").write_text("new")
            os.replace(a / "replacement", a / "skill/SKILL.md")
            (a / "skill/new.txt").write_text("created")
            # 确认由挂载处理的实际提交产生通知。
            assert receive_a()["type"] in {"changed", "resync_required"}
            invalidate("/_runtime", True)
            with (b / "_runtime/unrelated.txt").open("w") as unrelated:
                unrelated.write("writer outside subscribed skill")
                unrelated.flush()
                os.fsync(unrelated.fileno())
                response = request_invalidation("/", True, ["skill/**", "moved/**"])
                assert response["ok"], response
                invalidate("/skill", True)
                assert (b / "skill/SKILL.md").read_text() == "new"
                assert (b / "skill/new.txt").read_text() == "created"
            (a / "skill/SKILL.md").unlink()
            invalidate("/skill/SKILL.md")
            assert not (b / "skill/SKILL.md").exists()
            archive = root / "incoming.zip"
            with zipfile.ZipFile(archive, "w") as package:
                package.writestr("package/SKILL.md", "installed skill")
                package.writestr("package/data.txt", "long content")
            assert not (b / "skill/package").exists()
            with zipfile.ZipFile(archive) as package:
                package.extractall(a / "skill")
            invalidate("/skill", True)
            assert (b / "skill/package/SKILL.md").read_text() == "installed skill"
            assert (b / "skill/package/data.txt").read_text() == "long content"
            with (a / "skill/package/data.txt").open("r+") as stream:
                stream.truncate(4)
            invalidate("/skill/package/data.txt")
            assert (b / "skill/package/data.txt").read_text() == "long"
            os.link(a / "skill/package/data.txt", a / "skill/package/alias.txt")
            invalidate("/skill/package", True)
            assert (b / "skill/package/alias.txt").read_text() == "long"
            (a / "skill/package/data.txt").write_text("changed through shared inode")
            invalidate("/skill/package/data.txt")
            assert (b / "skill/package/alias.txt").read_text() == "changed through shared inode"
            assert not (b / "moved").exists()
            os.rename(a / "skill/package", a / "moved")
            invalidate("/skill/package", True)
            invalidate("/moved", True)
            assert not (b / "skill/package").exists()
            assert (b / "moved/SKILL.md").read_text() == "installed skill"
            # 接入端断开后，挂载仍须自己回收历史定位。
            connection_a.shutdown(socket.SHUT_RDWR)
            connection_a.close()
            def metrics(mount):
                return {
                    key: float(value)
                    for key, value in (
                        line.split() for line in (mount / ".stats").read_text().splitlines()
                    )
                    if key.startswith("juicefs_change_notification_")
                }
            baseline = metrics(a)["juicefs_change_notification_path_records"]
            for index in range(200):
                content = f"replacement-{index}"
                with (a / "replacement").open("w") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(a / "replacement", a / "moved/SKILL.md")
                invalidate("/moved/SKILL.md")
                assert (b / "moved/SKILL.md").read_text() == content
            deadline = time.monotonic() + 30
            while True:
                observed = metrics(a)
                assert observed["juicefs_change_notification_index_incomplete"] == 0, observed
                if observed["juicefs_change_notification_path_retired"] == 0:
                    break
                assert time.monotonic() < deadline, observed
                time.sleep(0.1)
            assert observed["juicefs_change_notification_path_records"] <= baseline + 4, observed
            assert observed["juicefs_change_notification_path_reclaimed_total"] >= 199, observed
            connection_b.shutdown(socket.SHUT_RDWR)
            connection_b.close()
            print("多挂载的缓存失效、连续替换与断连后的索引回收验收通过")
        except BaseException:
            for name in ("a", "b"):
                log_path = root / (name + ".log")
                if log_path.exists():
                    print(f"挂载 {name} 失败诊断（末尾 64 KiB）：\n{log_path.read_text()[-65536:]}")
            raise
        finally:
            for mount in mounts:
                subprocess.run([binary, "umount", str(mount)], check=False)
            for process in processes:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()


if __name__ == "__main__":
    main()
