#!/usr/bin/env python3
"""Exercise a native receiver with independently pinned Docker logging clients.

Requires Python 3.9+, Docker, OpenSSL and a built Logbrook binary. All producer
content and credentials are synthetic. The script owns and removes its containers,
server process, certificates and temporary database, including on failure.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

FLUENTD_IMAGE = "fluent/fluentd@sha256:188ca130e9998adb2587cfa09416336b6d3bc309153b4564c48f9023fda12124"
FLUENTBIT_IMAGE = "fluent/fluent-bit@sha256:2740f387f6e68fab66d28e4fa6a853387b9cab6ffb4cb78958f69bba4eb0e7b4"
LABEL = "logbrook.forward-interop"


def hostname(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
        if address.version != 4:
            raise argparse.ArgumentTypeError("this native harness requires an IPv4 address or DNS name")
        return value
    except ValueError:
        pass
    if len(value) > 253 or any(
        not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
        for label in value.split(".")
    ):
        raise argparse.ArgumentTypeError("host must be a plain DNS name or IP address")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def tail(path: Path, maximum: int = 32768) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as source:
        source.seek(max(0, path.stat().st_size - maximum))
        return source.read(maximum).decode("utf-8", "replace")


def quoted(value: object) -> str:
    # JSON string escaping is also valid for these TOML basic-string inputs.
    return json.dumps(str(value))


class AckFaultProxy:
    """Bounded plaintext negative control; corrupt only receiver-to-sender ACKs."""

    def __init__(self, backend_port: int):
        self.backend_port = backend_port
        self.listener = socket.socket()
        self.listener.bind(("0.0.0.0", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.sockets = []
        self.threads = []
        self.corrupted_responses = 0
        self.connections = 0
        self.acceptor = threading.Thread(target=self.accept, daemon=True)
        self.acceptor.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connections += 1
            if self.connections > 8:
                client.close()
                continue
            worker = threading.Thread(target=self.relay, args=(client,), daemon=True)
            self.threads.append(worker)
            worker.start()

    def relay(self, client):
        backend = None
        try:
            backend = socket.create_connection(("127.0.0.1", self.backend_port), timeout=5)
            client.settimeout(0.2)
            backend.settimeout(0.2)
            with self.lock:
                self.sockets.extend([client, backend])
            def upstream():
                try:
                    while not self.stop.is_set():
                        try:
                            data = client.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            return
                        backend.sendall(data)
                except OSError:
                    pass
            sender = threading.Thread(target=upstream, daemon=True)
            self.threads.append(sender)
            sender.start()
            while not self.stop.is_set():
                try:
                    response = backend.recv(4096)
                except socket.timeout:
                    continue
                if not response:
                    return
                # Trusted-local collectors have no handshake: all responses are ACKs.
                # Reserved MessagePack marker 0xc1 makes the negative control unambiguous.
                with self.lock:
                    self.corrupted_responses += 1
                client.sendall(b"\xc1" + response[1:])
        except OSError:
            pass
        finally:
            client.close()
            if backend:
                backend.close()

    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            for connection in self.sockets:
                try:
                    connection.close()
                except OSError:
                    pass
        deadline = time.monotonic() + 2
        for thread in [self.acceptor, *self.threads]:
            thread.join(timeout=max(0, deadline - time.monotonic()))


class Harness:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.run_id = secrets.token_hex(6)
        self.temporary = tempfile.TemporaryDirectory(prefix="logbrook-forward-interop-")
        self.root = Path(self.temporary.name)
        self.clients = self.root / "clients"
        self.clients.mkdir(mode=0o755)
        self.clients.chmod(0o755)
        self.private = self.root / "private"
        self.private.mkdir(mode=0o700)
        self.read_token = secrets.token_urlsafe(24)
        self.write_token = secrets.token_urlsafe(24)
        self.shared_key = secrets.token_urlsafe(24)
        self.password = secrets.token_urlsafe(24)
        self.secrets = [self.read_token, self.write_token, self.shared_key, self.password]
        self.containers: set[str] = set()
        self.server: subprocess.Popen | None = None
        self.server_log = None
        self.reservations: list[socket.socket] = []
        self.proxy: AckFaultProxy | None = None
        self.started_ms = int(time.time() * 1000)
        self.markers = {
            "docker_stdout": f"interop-{self.run_id}-docker-stdout",
            "docker_stderr": f"interop-{self.run_id}-docker-stderr",
            "fluentd_gzip_ack": f"interop-{self.run_id}-fluentd-gzip",
            "fluentbit_gzip_ack": f"interop-{self.run_id}-fluentbit-gzip",
            "fluentd_tls_password_ack": f"interop-{self.run_id}-fluentd-secure",
        }
        self.report: dict = {
            "schema_version": 1,
            "run_id": self.run_id,
            "passed": False,
            "parameters": {
                "binary": str(args.binary), "host": args.host,
                "network": args.network, "observation_timeout_seconds": args.timeout,
            },
            "binary_sha256": sha256(args.binary),
            "harness_sha256": sha256(Path(__file__)),
            "platform": {"system": sys.platform, "machine": os.uname().machine},
            "clients": {}, "cleanup": {}, "cases": {},
            "negative_control": {"client": args.negative_control, "responses_corrupted": 0},
        }

    def redact(self, value: object) -> str:
        text = str(value)
        for secret in self.secrets:
            text = text.replace(secret, "[redacted]")
        return text

    def command(self, arguments: list[str], timeout: float = 180, combine: bool = False) -> str:
        result = subprocess.run(
            arguments, capture_output=True, text=True, timeout=timeout, check=False
        )
        if result.returncode:
            detail = self.redact((result.stdout + result.stderr)[-32768:])
            raise RuntimeError(f"{arguments[0]} {arguments[1]} failed ({result.returncode}): {detail}")
        return (result.stdout + (result.stderr if combine else "")).strip()

    def docker(self, *arguments: str, timeout: float = 180) -> str:
        return self.command(["docker", *arguments], timeout=timeout, combine=arguments[0] == "logs")

    def container(self, suffix: str, image: str, arguments: list[str], *, detached: bool = True,
                  extra: list[str] | None = None) -> str:
        name = f"logbrook-forward-{self.run_id}-{suffix}"
        self.containers.add(name)
        command = ["run", "--name", name, "--label", f"{LABEL}={self.run_id}"]
        if detached:
            command.append("-d")
        if self.args.network == "host":
            command += ["--network", "host"]
        if not extra or "--log-driver" not in extra:
            # Do not inherit a developer daemon's remote default logging driver.
            command += ["--log-driver", "json-file", "--log-opt", "max-size=1m", "--log-opt", "max-file=1"]
        if extra:
            command += extra
        command += [image, *arguments]
        self.docker(*command)
        return name

    def reserve_port(self) -> int:
        reservation = socket.socket()
        reservation.bind(("0.0.0.0", 0))
        self.reservations.append(reservation)
        return reservation.getsockname()[1]

    def image(self, kind: str, reference: str, version_arguments: list[str]) -> None:
        self.docker("pull", reference)
        details = json.loads(self.docker("image", "inspect", reference))[0]
        name = self.container(f"version-{kind}", reference, version_arguments, detached=False)
        version = self.docker("logs", name, timeout=15)
        self.report["clients"][kind] = {
            "reference": reference,
            "image_id": details["Id"], "repo_digests": details.get("RepoDigests", []),
            "os": details["Os"], "architecture": details["Architecture"],
            "version": self.redact(version),
        }

    def write(self, path: Path, text: str, mode: int = 0o600) -> None:
        path.write_text(text, encoding="utf-8")
        path.chmod(mode)

    def certificate(self) -> None:
        try:
            ipaddress.ip_address(self.args.host)
            san = f"IP:{self.args.host}"
        except ValueError:
            san = f"DNS:{self.args.host}"
        configuration = self.private / "openssl.cnf"
        self.write(configuration, f"""[req]
distinguished_name=dn
x509_extensions=ext
prompt=no
[dn]
CN={self.args.host}
[ext]
subjectAltName={san}
basicConstraints=critical,CA:TRUE
keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage=serverAuth
""")
        self.command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                      "-keyout", str(self.private / "server.key"),
                      "-out", str(self.private / "server.pem"), "-days", "1",
                      "-config", str(configuration)], timeout=30)
        (self.private / "server.key").chmod(0o600)
        shutil.copyfile(self.private / "server.pem", self.clients / "ca.pem")
        (self.clients / "ca.pem").chmod(0o644)

    def configuration(self) -> None:
        self.http_port, self.forward_port, self.secure_port = [self.reserve_port() for _ in range(3)]
        self.base = f"http://127.0.0.1:{self.http_port}"
        if self.args.negative_control:
            self.proxy = AckFaultProxy(self.forward_port)
        self.write(self.private / "shared-key", self.shared_key)
        self.write(self.private / "password", self.password)
        self.certificate()
        configuration = f"""bind={quoted(f'127.0.0.1:{self.http_port}')}
max_indexes=1
[storage]
data_dir={quoted(self.root / 'data')}
memory_limit="256MB"
[ingest_tokens]
{quoted(self.write_token)}="http"
[read_tokens]
{quoted(self.read_token)}=[]
[forward]
enabled=true
[[forward.listeners]]
name="public"
bind={quoted(f'0.0.0.0:{self.forward_port}')}
security="trusted_local"
principal="public"
trusted_network=true
[[forward.listeners]]
name="secure"
bind={quoted(f'0.0.0.0:{self.secure_port}')}
security="shared_key"
server_hostname="logbrook-interop"
[forward.listeners.shared_key]
file={quoted(self.private / 'shared-key')}
[forward.listeners.users.fixture]
principal="secure"
[forward.listeners.users.fixture.password]
file={quoted(self.private / 'password')}
[forward.listeners.tls]
certificate={quoted(self.private / 'server.pem')}
private_key={quoted(self.private / 'server.key')}
[forward.principals.public]
source="interop-public"
indexes=["default"]
[forward.principals.secure]
source="interop-secure"
indexes=["default"]
[[forward.routes]]
listener="public"
tag_prefix="interop.docker."
index="default"
normalizer="docker"
json_profile="pino"
[[forward.routes]]
listener="public"
tag_prefix="interop.collector."
index="default"
normalizer="generic"
[[forward.routes]]
listener="secure"
tag_prefix="interop.secure."
index="default"
normalizer="generic"
"""
        self.write(self.private / "logbrook.toml", configuration)
        self.report["receiver_configuration"] = self.redact(configuration)
        for kind, secure in [("fluentd_gzip_ack", False), ("fluentd_tls_password_ack", True)]:
            tag = f"interop.{'secure' if secure else 'collector'}.{self.run_id}"
            transport = "transport tls\n  tls_cert_path /test/ca.pem" if secure else ""
            port = self.secure_port if secure else self.forward_port
            if not secure and self.args.negative_control == "fluentd":
                port = self.proxy.port
            user = f"username fixture\n    password {self.password}" if secure else ""
            security = f"""<security>
    self_hostname fluentd-interop
    shared_key {self.shared_key}
  </security>""" if secure else ""
            record = json.dumps({"message": self.markers[kind], "fixture": kind})
            emitter = "\n".join(["#!/bin/sh", f"printf '%s\\n' {shlex.quote(record)}", "sleep 2",
                                  f"printf '%s\\n' {shlex.quote(record)}", "sleep 2",
                                  f"printf '%s\\n' {shlex.quote(record)}", ""])
            self.write(self.clients / f"{kind}.sh", emitter, 0o644)
            text = f"""<source>
  @type exec
  command /bin/sh /test/{kind}.sh
  tag {tag}
  <parse>
    @type json
  </parse>
</source>
<match {tag}>
  @type forward
  {transport}
  require_ack_response true
  heartbeat_type none
  ack_response_timeout 5s
  compress gzip
  <server>
    host {self.args.host}
    port {port}
    {user}
  </server>
  {security}
  <buffer>
    flush_interval 1s
    retry_max_times 1
  </buffer>
</match>
"""
            self.write(self.clients / f"{kind}.conf", text, 0o644)
        self.write(self.clients / "fluentbit.conf", f"""[SERVICE]
    Flush 1
[INPUT]
    Name dummy
    Tag interop.collector.{self.run_id}
    Dummy {{"message":"{self.markers['fluentbit_gzip_ack']}","fixture":"fluentbit"}}
    Rate 1
    Samples 6
[OUTPUT]
    Name forward
    Match *
    Host {self.args.host}
    Port {self.proxy.port if self.args.negative_control == "fluentbit" else self.forward_port}
    Require_ack_response On
    Compress gzip
""", 0o644)

    def get(self, path: str, raw: bool = False):
        request = urllib.request.Request(self.base + path, headers={"Authorization": f"Bearer {self.read_token}"})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=5) as response:
            content = response.read(8 * 1024 * 1024 + 1)
        if len(content) > 8 * 1024 * 1024:
            raise RuntimeError("interop HTTP response exceeded its bound")
        return content.decode() if raw else json.loads(content)

    def until(self, predicate, description: str, timeout: float | None = None):
        deadline = time.monotonic() + (self.args.timeout if timeout is None else timeout)
        last = None
        while time.monotonic() < deadline:
            if self.server is not None and self.server.poll() is not None:
                raise RuntimeError(f"receiver exited ({self.server.returncode}): {self.redact(tail(self.root / 'server.log'))}")
            try:
                result = predicate()
                if result:
                    return result
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last = error
            time.sleep(0.1)
        raise RuntimeError(f"deadline waiting for {description}; last network error: {last}")

    def start(self) -> None:
        for reservation in self.reservations:
            reservation.close()
        self.reservations.clear()
        self.server_log = (self.root / "server.log").open("w")
        environment = {key: value for key, value in os.environ.items() if not key.startswith("LOGBROOK_")}
        self.server = subprocess.Popen(
            [str(self.args.binary), "--config", str(self.private / "logbrook.toml"), "serve"],
            stdout=self.server_log, stderr=self.server_log, env=environment,
        )
        self.until(lambda: self.get("/ready"), "native receiver readiness", timeout=30)

    def metrics(self) -> dict[str, float]:
        parsed = {}
        for line in self.get("/metrics", raw=True).splitlines():
            if line and not line.startswith("#"):
                key, value = line.rsplit(" ", 1)
                parsed[key] = float(value)
        return parsed

    def inspect_sender(self, name: str) -> tuple[int | None, str]:
        state = json.loads(self.docker("inspect", "--format", "{{json .State}}", name, timeout=10))
        logs = self.redact(self.docker("logs", "--tail", "120", name, timeout=10))
        return None if state["Running"] else state["ExitCode"], logs

    @staticmethod
    def sender_errors(logs: str) -> bool:
        return bool(re.search(r"\[\s*error\s*\]|error_class=|failed to flush|failed to send|"
                              r"retry_times=|retry_count=|no response from|parameter .+not used", logs, re.IGNORECASE))

    def verify_rows(self, kind: str) -> list[dict]:
        matching = [row for row in self.events() if row["message"] == self.markers[kind]]
        expected_source = "interop-secure" if kind == "fluentd_tls_password_ack" else "interop-public"
        if not matching or not all(row["source"] == expected_source for row in matching):
            raise RuntimeError(f"missing records or incorrect authenticated source: {kind}")
        if kind.startswith("docker_"):
            if len(matching) != 1:
                raise RuntimeError(f"Docker driver duplicated a marker: {kind}")
            stream = "stdout" if kind == "docker_stdout" else "stderr"
            for row in matching:
                forward = row["attributes"]["forward"]
                nanos = forward["nanoseconds"]
                if not 0 < nanos < 1_000_000_000 or row["event_time"] != forward["seconds"] * 1000 + nanos // 1_000_000:
                    raise RuntimeError(f"Docker EventTime/subsecond mapping failed: {kind}")
                if row["attributes"]["docker"]["stream"] != stream or row["level"] != (30 if stream == "stdout" else 50):
                    raise RuntimeError(f"Docker stream/severity mapping failed: {kind}")
        elif not all(row["attributes"]["forward"]["mode"] == "CompressedPackedForward" for row in matching):
            raise RuntimeError(f"collector did not use gzip PackedForward: {kind}")
        if kind == "fluentbit_gzip_ack":
            if not all(row["attributes"]["forward"]["fluent_signal"] == 0 and
                       "original_metadata_msgpack_b64" in row["attributes"]["forward"] for row in matching):
                raise RuntimeError("Fluent Bit declared logs metadata was not preserved")
        if not kind.startswith("docker_"):
            timestamps = {(row["attributes"]["forward"]["seconds"], row["attributes"]["forward"]["nanoseconds"]) for row in matching}
            if len(timestamps) != len(matching):
                self.report["cases"].setdefault(kind, {})["sender_repeated_chunk"] = True
                raise RuntimeError(f"sender repeated an already committed chunk: {kind}")
        return matching

    def case(self, kind: str, start, *, docker_driver: bool = False) -> None:
        self.report["failed_case"] = kind
        case = {"passed": False}
        self.report["cases"][kind] = case
        before = self.metrics()
        begun = time.monotonic()
        name = start()
        minimum_acks = 2 if docker_driver else 3
        expected_records = 6 if kind == "fluentbit_gzip_ack" else 3
        stable_since = None
        def advanced():
            nonlocal stable_since
            metrics = self.metrics()
            exit_code, logs = self.inspect_sender(name)
            if self.sender_errors(logs):
                case["sender_error_detected"] = True
                case["sender_logs"] = logs
                raise RuntimeError(f"sender ACK/flush/retry diagnostics failed: {kind}")
            if exit_code is not None and (not docker_driver or exit_code != 0):
                raise RuntimeError(f"sender exited unexpectedly: {kind}: {exit_code}")
            observed = metrics.get("logbrook_forward_ack_sent_total", 0) - before.get("logbrook_forward_ack_sent_total", 0)
            if observed < minimum_acks or (not docker_driver and time.monotonic() - begun < 10):
                return False
            kinds = ["docker_stdout", "docker_stderr"] if docker_driver else [kind]
            for entry in kinds:
                rows = self.verify_rows(entry)
                if not docker_driver:
                    timestamps = {(row["attributes"]["forward"]["seconds"], row["attributes"]["forward"]["nanoseconds"]) for row in rows}
                    if len(timestamps) != len(rows):
                        case["sender_repeated_chunk"] = True
                        raise RuntimeError(f"sender repeated an already committed chunk: {kind}")
                    if len(rows) < expected_records:
                        return False
            if not docker_driver:
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since < 8:
                    return False
            return True
        self.until(advanced, f"isolated {kind} ACKs and independent successful flushes")
        if not docker_driver:
            self.docker("stop", "--time", "15", name, timeout=25)
        exit_code, logs = self.inspect_sender(name)
        if exit_code != 0 or self.sender_errors(logs):
            case["sender_error_detected"] = self.sender_errors(logs)
            raise RuntimeError(f"sender did not stop with clean flushed buffers: {kind}: {exit_code}")
        self.until(lambda: self.metrics().get("logbrook_forward_connections_active") == 0,
                   f"{kind} receiver session drain", timeout=15)
        after = self.metrics()
        delta = {name: int(after.get(name, 0) - before.get(name, 0)) for name in [
            "logbrook_forward_ack_requested_total", "logbrook_forward_ack_sent_total",
            "logbrook_forward_authentication_rejected_total", "logbrook_forward_events_committed_total",
        ]}
        if delta["logbrook_forward_ack_requested_total"] < minimum_acks or delta["logbrook_forward_ack_sent_total"] < minimum_acks:
            raise RuntimeError(f"isolated sender ACK deltas were incomplete: {kind}")
        if delta["logbrook_forward_authentication_rejected_total"]:
            raise RuntimeError(f"reference sender authentication was rejected: {kind}")
        if docker_driver and (delta["logbrook_forward_ack_requested_total"] != 2 or delta["logbrook_forward_ack_sent_total"] != 2):
            raise RuntimeError("Docker driver did not produce exactly two isolated ACKs")
        examples = {}
        for entry in (["docker_stdout", "docker_stderr"] if docker_driver else [kind]):
            rows = self.verify_rows(entry)
            if not docker_driver and len(rows) != expected_records:
                raise RuntimeError(f"finite sender record count changed during shutdown: {kind}")
            examples[entry] = {"queried_events": len(rows), "example": rows[0]}
        case.update({"passed": True, "ack_counter_deltas": delta, "sender_exit_code": exit_code,
                     "sender_logs": logs, "elapsed_seconds": time.monotonic() - begun})
        for entry, example in examples.items():
            self.report["cases"][entry] = {**case, **example}
        self.report.pop("failed_case", None)

    def producers(self) -> None:
        stdout = json.dumps({"level": 30, "msg": self.markers["docker_stdout"]})
        stderr = json.dumps({"level": 50, "msg": self.markers["docker_stderr"]})
        script = f"printf '%s\\n' {shlex.quote(stdout)}; printf '%s\\n' {shlex.quote(stderr)} >&2; sleep 2"
        host = f"[{self.args.host}]" if ":" in self.args.host else self.args.host
        def driver():
            name = self.container("docker", self.args.fluentd_image, ["-c", script], detached=False, extra=[
                "--entrypoint", "sh", "--log-driver", "fluentd", "--log-opt", f"fluentd-address={host}:{self.forward_port}",
                "--log-opt", "fluentd-request-ack=true", "--log-opt", "fluentd-sub-second-precision=true",
                "--log-opt", f"tag=interop.docker.{self.run_id}",
            ])
            self.report["docker_logging_configuration"] = json.loads(self.docker("inspect", "--format", "{{json .HostConfig.LogConfig}}", name))
            return name
        self.case("docker_driver", driver, docker_driver=True)
        for kind, image, configuration in [
            ("fluentd_gzip_ack", self.args.fluentd_image, "fluentd_gzip_ack.conf"),
            ("fluentbit_gzip_ack", self.args.fluentbit_image, "fluentbit.conf"),
            ("fluentd_tls_password_ack", self.args.fluentd_image, "fluentd_tls_password_ack.conf"),
        ]:
            self.case(kind, lambda kind=kind, image=image, configuration=configuration:
                      self.container(kind, image, ["-c", f"/test/{configuration}"], extra=["-v", f"{self.clients}:/test:ro"]))
        self.report["metrics"] = self.get("/metrics", raw=True)
        self.report["ack_semantics"] = "Clients run in isolation. Per-case ACK deltas, at least three independent collector flushes, observation beyond Fluentd's short ACK/delayed-commit timeout, sender retry/error diagnostics and clean flushed shutdown are required. Corrupt-ACK negative controls test qualification sensitivity; crash/response-loss guarantees remain in forward_process."

    def events(self) -> list[dict]:
        query = urllib.parse.urlencode({"from": self.started_ms - 1000, "to": int(time.time() * 1000) + 1000,
                                       "message": f"interop-{self.run_id}-", "limit": 1000})
        return self.get(f"/logs?{query}")["events"]

    def cleanup(self) -> None:
        errors = []
        logs = {}
        for name in sorted(self.containers):
            try:
                owner = self.docker("inspect", "--format", f'{{{{index .Config.Labels "{LABEL}"}}}}', name, timeout=10)
                if owner != self.run_id:
                    errors.append(f"refusing to remove container with unexpected ownership: {name}")
                    continue
            except Exception as error:
                errors.append(self.redact(error))
                continue
            try:
                logs[name] = self.redact(self.docker("logs", "--tail", "80", name, timeout=10))
            except Exception as error:
                errors.append(self.redact(error))
            try:
                self.docker("rm", "-f", name, timeout=20)
            except Exception as error:
                errors.append(self.redact(error))
        self.report["client_logs"] = logs
        if self.proxy:
            self.proxy.close()
            self.report["negative_control"]["responses_corrupted"] = self.proxy.corrupted_responses
        for reservation in self.reservations:
            reservation.close()
        if self.server is not None:
            try:
                if self.server.poll() is None:
                    self.server.terminate()
                    try:
                        self.server.wait(timeout=35)
                    except subprocess.TimeoutExpired:
                        self.server.kill()
                        self.server.wait(timeout=5)
                        errors.append("native receiver exceeded its graceful stop deadline")
                if self.server.returncode != 0:
                    errors.append(f"native receiver exit code {self.server.returncode}")
                self.report["cleanup"]["native_exit_code"] = self.server.returncode
            except Exception as error:
                errors.append(self.redact(error))
                try:
                    self.server.kill()
                    self.server.wait(timeout=5)
                except Exception as kill_error:
                    errors.append(self.redact(kill_error))
        if self.server_log:
            self.server_log.close()
        self.report["server_log"] = self.redact(tail(self.root / "server.log"))
        try:
            self.temporary.cleanup()
        except Exception as error:
            errors.append(self.redact(error))
        self.report["cleanup"].update({"temporary_data_removed": not self.root.exists(), "errors": errors})

    def run(self) -> dict:
        try:
            self.report["docker_version"] = json.loads(self.docker("version", "--format", "{{json .}}"))
            self.image("fluentd", self.args.fluentd_image, ["--version"])
            self.image("fluentbit", self.args.fluentbit_image, ["--version"])
            self.configuration()
            self.start()
            self.producers()
            self.report["passed"] = True
        except Exception as error:
            self.report["error"] = self.redact(f"{type(error).__name__}: {error}")
            if self.server is not None and self.server.poll() is None:
                try:
                    self.report["metrics_on_failure"] = self.get("/metrics", raw=True)
                    self.report["events_on_failure"] = self.events()
                except Exception:
                    pass
        finally:
            # A second signal cannot interrupt cleanup halfway through its owned resources.
            for number in [signal.SIGINT, signal.SIGTERM]:
                signal.signal(number, signal.SIG_IGN)
            self.cleanup()
        self.report["passed"] &= not self.report["cleanup"]["errors"]
        return self.report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/logbrook"))
    parser.add_argument("--host", type=hostname, default="host.docker.internal",
                        help="hostname/IP reachable from both Docker's logging daemon and client containers")
    parser.add_argument("--network", choices=["bridge", "host"], default="bridge",
                        help="use host with --host 127.0.0.1 on native Linux Docker")
    parser.add_argument("--fluentd-image", default=FLUENTD_IMAGE)
    parser.add_argument("--fluentbit-image", default=FLUENTBIT_IMAGE)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--negative-control", choices=["fluentd", "fluentbit"],
                        help="corrupt one plaintext collector's ACKs; a qualification failure is expected")
    parser.add_argument("--output", type=Path, default=Path("artifacts/forward/interop.json"))
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error("--binary must name a built executable")
    if not 30 <= args.timeout <= 300:
        parser.error("--timeout must be between 30 and 300 seconds")
    for command in ["docker", "openssl"]:
        if shutil.which(command) is None:
            parser.error(f"required command not found: {command}")
    def interrupted(_number, _frame):
        raise InterruptedError("interoperability check interrupted")
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, interrupted)
    report = Harness(args).run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "output": str(args.output), "error": report.get("error")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
