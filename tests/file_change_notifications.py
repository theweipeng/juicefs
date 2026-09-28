#!/usr/bin/env python3
"""Linux 多挂载的真实缓存失效验收。"""

import argparse
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
    send({"type": "subscribe", "id": "subscribe", "patterns": ["**"]})
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
                process = subprocess.Popen([binary, "mount", "--cache-dir", str(root / (name + "-cache")), "--attr-cache=180", "--entry-cache=180", "--dir-entry-cache=180", "--negative-entry-cache=180", "--open-cache=180", "--change-socket", str(root / (name + ".sock")), "--change-volume=test", "--change-glob=**", "--backup-meta=0", "--no-usage-report", metadata, str(mount)])
                processes.append(process)
            deadline = time.monotonic() + 30
            while not all(os.path.ismount(mount) for mount in mounts):
                if time.monotonic() > deadline or any(process.poll() is not None for process in processes):
                    raise RuntimeError("挂载失败")
                time.sleep(0.1)
            a, b = mounts
            connection_a, _, receive_a = connect(root / "a.sock")
            connection_b, send_b, receive_b = connect(root / "b.sock")
            def invalidate(path, recursive=False):
                send_b({"type": "invalidate", "id": "invalidate", "changes": [{"path": path, "recursive": recursive}]})
                while True:
                    response = receive_b()
                    if response.get("id") == "invalidate":
                        assert response["ok"], response
                        return
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
