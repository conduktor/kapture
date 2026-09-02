#!/usr/bin/env python3
"""Rootful end-to-end smoke test for the PID-scoped OpenSSL tap."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import time


FRAME_HEADER = struct.Struct("<BQQII")
LENGTH_PREFIX = struct.Struct("!I")
WRITE = 0
READ = 1
HEALTH = 2
REQUEST_BODY = b"KAPTURE-REQUEST-BEGIN\n" + (b"q" * 20_000) + b"\nKAPTURE-REQUEST-END"
RESPONSE_BODY = b"KAPTURE-RESPONSE-BEGIN\n" + (b"r" * 70_000) + b"\nKAPTURE-RESPONSE-END"
OVERSIZE_BODY = b"KAPTURE-OVERSIZE-BEGIN\n" + (b"x" * (1024 * 1024)) + b"\nKAPTURE-OVERSIZE-END"


def receive_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise EOFError(f"unexpected EOF with {remaining} byte(s) outstanding")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def target(cert: str, key: str, port: int) -> int:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(1)
        print("READY", flush=True)
        connection, _ = listener.accept()
        with connection, context.wrap_socket(connection, server_side=True) as tls:
            request_length = LENGTH_PREFIX.unpack(receive_exact(tls, LENGTH_PREFIX.size))[0]
            request = receive_exact(tls, request_length)
            if request != REQUEST_BODY:
                raise RuntimeError("TLS target received a corrupted request")
            tls.sendall(LENGTH_PREFIX.pack(len(RESPONSE_BODY)) + RESPONSE_BODY)
            # A distinct SSL_write_ex call above the tap's bounded 1 MiB
            # per-call budget must invalidate capture without emitting a
            # partial event. The TLS peer must remain unaffected.
            tls.sendall(LENGTH_PREFIX.pack(len(OVERSIZE_BODY)) + OVERSIZE_BODY)
    return 0


def available_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def mapped_libssl(pid: int) -> str:
    for line in Path(f"/proc/{pid}/maps").read_text(encoding="utf-8").splitlines():
        candidate = line.rsplit(maxsplit=1)[-1]
        if "/libssl.so" in candidate:
            return candidate
    raise RuntimeError(f"PID {pid} does not map a shared libssl")


def wait_for_ready(process: subprocess.Popen[str]) -> None:
    if process.stdout is None or process.stdout.readline().strip() != "READY":
        stderr = process.stderr.read() if process.stderr else ""
        raise RuntimeError(f"TLS target did not become ready: {stderr}")


def terminate(process: subprocess.Popen[str] | None) -> tuple[int, str]:
    if process is None:
        return 0, ""
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        _, stderr = process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate(timeout=5)
    return process.returncode, stderr


def exercise(loader_path: Path) -> None:
    if os.geteuid() != 0:
        raise RuntimeError("rootful smoke requires root or equivalent BPF capabilities")
    namespace_ids = next(
        line.split()[1:]
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
        if line.startswith("NSpid:")
    )
    if len(namespace_ids) != 1:
        raise RuntimeError(
            "rootful smoke must run in the initial PID namespace; use Docker --pid=host"
        )
    if not loader_path.is_file():
        raise RuntimeError(f"loader does not exist: {loader_path}")

    tls_target: subprocess.Popen[str] | None = None
    loader: subprocess.Popen[str] | None = None
    with tempfile.TemporaryDirectory(prefix="kapture-ebpf-smoke-") as temp_dir:
        temporary = Path(temp_dir)
        cert = temporary / "cert.pem"
        key = temporary / "key.pem"
        socket_path = temporary / "tap.sock"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        listener.settimeout(10)
        try:
            port = available_port()
            tls_target = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--target",
                    str(cert),
                    str(key),
                    str(port),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            wait_for_ready(tls_target)
            library = mapped_libssl(tls_target.pid)
            loader = subprocess.Popen(
                [
                    str(loader_path),
                    "--pid",
                    str(tls_target.pid),
                    "--library",
                    library,
                    "--socket",
                    str(socket_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                tap_connection, _ = listener.accept()
            except TimeoutError as error:
                loader_code, loader_stderr = terminate(loader)
                loader = None
                raise RuntimeError(
                    f"tap loader did not connect (exit={loader_code}): {loader_stderr}"
                ) from error
            tap_connection.settimeout(0.25)

            client_context = ssl.create_default_context()
            client_context.check_hostname = False
            client_context.verify_mode = ssl.CERT_NONE
            with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
                with client_context.wrap_socket(connection, server_hostname="localhost") as tls:
                    tls.sendall(LENGTH_PREFIX.pack(len(REQUEST_BODY)) + REQUEST_BODY)
                    response_length = LENGTH_PREFIX.unpack(
                        receive_exact(tls, LENGTH_PREFIX.size)
                    )[0]
                    response = receive_exact(tls, response_length)
                    if response != RESPONSE_BODY:
                        raise RuntimeError("TLS client received a corrupted response")
                    oversize_length = LENGTH_PREFIX.unpack(
                        receive_exact(tls, LENGTH_PREFIX.size)
                    )[0]
                    oversize_response = receive_exact(tls, oversize_length)
                    if oversize_response != OVERSIZE_BODY:
                        raise RuntimeError("TLS client received a corrupted oversize response")

            streams: dict[tuple[int, int], bytearray] = {}
            lengths: dict[int, list[int]] = {WRITE: [], READ: []}
            health_frames: list[int] = []
            deadline = time.monotonic() + 10
            expected_request = LENGTH_PREFIX.pack(len(REQUEST_BODY)) + REQUEST_BODY
            expected_response = LENGTH_PREFIX.pack(len(RESPONSE_BODY)) + RESPONSE_BODY
            while time.monotonic() < deadline:
                try:
                    raw_header = receive_exact(tap_connection, FRAME_HEADER.size)
                except EOFError:
                    break
                except socket.timeout:
                    if health_frames == [0, 1] and loader.poll() is not None:
                        break
                    continue
                direction, observed, emitted, connection_id, length = FRAME_HEADER.unpack(raw_header)
                if observed == 0 or emitted == 0:
                    raise RuntimeError("tap emitted a frame without monotonic timestamps")
                payload = receive_exact(tap_connection, length)
                if direction == HEALTH:
                    if length != 8:
                        raise RuntimeError(f"invalid health frame length: {length}")
                    health_frames.append(struct.unpack("<Q", payload)[0])
                    continue
                if direction not in (WRITE, READ):
                    raise RuntimeError(f"invalid tap direction: {direction}")
                streams.setdefault((direction, connection_id), bytearray()).extend(payload)
                lengths[direction].append(length)
            tap_connection.close()

            target_code, target_stderr = terminate(tls_target)
            tls_target = None
            loader_code, loader_stderr = terminate(loader)
            loader = None
            if target_code != 0:
                raise RuntimeError(f"TLS target exited {target_code}: {target_stderr}")
            if loader_code == 0:
                raise RuntimeError("tap loader did not fail closed after an oversize call")

            if health_frames != [0, 1]:
                raise RuntimeError(
                    f"unexpected capture health sequence: {health_frames}; loader={loader_stderr}"
                )
            stream_sizes = {
                f"direction={direction},connection={connection_id}": len(value)
                for (direction, connection_id), value in streams.items()
            }
            if not any(
                expected_request in value
                for (direction, _), value in streams.items()
                if direction == READ
            ):
                raise RuntimeError(
                    "captured TLS reads do not contain the complete request; "
                    f"streams={stream_sizes}, chunks={lengths[READ]}, loader={loader_stderr}"
                )
            if not any(
                expected_response in value
                for (direction, _), value in streams.items()
                if direction == WRITE
            ):
                raise RuntimeError(
                    "captured TLS writes do not contain the complete 70 KiB response; "
                    f"streams={stream_sizes}, chunks={lengths[WRITE]}, loader={loader_stderr}"
                )
            if 16 * 1024 not in lengths[WRITE]:
                raise RuntimeError(
                    f"large SSL write was not split into 16 KiB chunks: {lengths[WRITE]}"
                )
            if any(
                b"KAPTURE-OVERSIZE-BEGIN" in value
                for (direction, _), value in streams.items()
                if direction == WRITE
            ):
                raise RuntimeError("tap forwarded partial data from an oversize SSL call")

            loss_match = re.search(
                r"events=(\d+) ring_drops=(\d+) read_faults=(\d+) oversize_calls=(\d+)",
                loader_stderr,
            )
            if not loss_match:
                raise RuntimeError(f"tap loader did not print capture counters: {loader_stderr}")
            _, ring_drops, read_faults, oversize_calls = map(int, loss_match.groups())
            if ring_drops != 0 or read_faults != 0 or oversize_calls != 1:
                raise RuntimeError(f"unexpected tap loss counters: {loss_match.group(0)}")
            runtime_stats = re.findall(
                r"program=ssl_(?:read|write)(?:_ex)?_(?:enter|exit) "
                r"runs=(\d+) runtime_ns=(\d+)",
                loader_stderr,
            )
            if not runtime_stats or sum(int(runs) for runs, _ in runtime_stats) == 0:
                raise RuntimeError(f"OpenSSL BPF run counters stayed at zero: {loader_stderr}")
            if sum(int(runtime) for _, runtime in runtime_stats) == 0:
                raise RuntimeError(f"OpenSSL BPF runtime stayed at zero: {loader_stderr}")
            print(
                "rootful smoke passed: "
                f"{loss_match.group(1)} events, {sum(lengths[READ])} read bytes, "
                f"{sum(lengths[WRITE])} write bytes, oversize fail-closed, "
                f"{sum(int(runtime) for _, runtime in runtime_stats)} BPF runtime ns"
            )
        finally:
            listener.close()
            terminate(loader)
            terminate(tls_target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loader", type=Path, default=Path("build/kapture-ebpf-tap"))
    parser.add_argument("--target", nargs=3, metavar=("CERT", "KEY", "PORT"))
    arguments = parser.parse_args()
    if arguments.target:
        cert, key, port = arguments.target
        return target(cert, key, int(port))
    exercise(arguments.loader.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
