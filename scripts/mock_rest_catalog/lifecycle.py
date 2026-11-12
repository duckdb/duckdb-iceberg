"""Background lifecycle for make mock / make mock-stop (Python standard library)."""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from .config import load_config
from .server import MockServer


ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / ".catalogs/mock"


def control(state, command):
    request = Request(
        state["uri"] + "/__mock/" + command,
        headers={"X-Mock-Token": state["token"]},
        method="POST" if command == "stop" else "GET",
    )
    with build_opener(ProxyHandler({})).open(request, timeout=2) as response:
        return json.load(response)


def serve(state_dir, directory, token, port):
    state_path = state_dir / "server.json"
    try:
        with MockServer(directory, port=port, control_token=token) as server:
            # Signal handlers wake the main thread; shutdown runs outside the serving thread.
            for signum in (signal.SIGTERM, signal.SIGINT):
                signal.signal(signum, lambda *_: server.stop_requested.set())
            state = {"uri": server.uri, "token": token, "pid": os.getpid(), "directory": str(directory)}
            temporary = state_dir / f"{token}.json"
            temporary.write_text(json.dumps(state))
            temporary.chmod(0o600)
            temporary.replace(state_path)
            print(f"Mock catalog listening at {server.uri}; warehouse: {server.catalog.warehouse}", flush=True)
            server.stop_requested.wait()
        if server.errors:
            (directory / "server-errors.log").write_text("\n".join(server.errors))
        print(f"Stopped; unsupported={len(server.unsupported)}, errors={len(server.errors)}", flush=True)
    finally:
        if state_path.exists() and json.loads(state_path.read_text())["token"] == token:
            state_path.unlink()


def start(state_dir):
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "server.json"
    if state_path.exists():
        raise RuntimeError("Mock state already exists; run make mock-stop first")
    _, uri = load_config()
    directory = Path(tempfile.mkdtemp(prefix="run-", dir=state_dir))
    token = secrets.token_hex(32)
    with (directory / "server.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "scripts.mock_rest_catalog.lifecycle",
                "serve",
                "--state-dir",
                str(state_dir),
                "--directory",
                str(directory),
                "--token",
                token,
                "--port",
                str(urlsplit(uri).port),
            ],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError((directory / "server.log").read_text())
            if state_path.exists():
                state = json.loads(state_path.read_text())
                if state["token"] == token:
                    control(state, "status")
                    print(f"Mock catalog ready at {uri}\nLogs and warehouse: {directory}")
                    return
            time.sleep(0.05)
        raise RuntimeError(f"Mock startup timed out; see {directory / 'server.log'}")
    except BaseException:
        # This is our own child, never a PID read from a possibly stale state file.
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise


def stop(state_dir):
    state_path = state_dir / "server.json"
    if not state_path.exists():
        print("Mock catalog is not running.")
        return
    state = json.loads(state_path.read_text())
    try:
        status = control(state, "status")
        control(state, "stop")
    except URLError as error:
        if isinstance(error.reason, ConnectionRefusedError):
            state_path.unlink()
            print("Removed stale mock state; server is no longer listening.")
            return
        raise
    deadline = time.monotonic() + 10
    while state_path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if state_path.exists():
        raise RuntimeError("Mock shutdown timed out; retained server state for inspection")
    print(f"Mock catalog stopped; unsupported={status['unsupported']}, errors={status['errors']}.")
    print(f"Logs and warehouse retained: {state['directory']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "serve"))
    parser.add_argument("--state-dir", type=Path, default=STATE_DIR)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--token")
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    if args.action == "serve":
        if args.directory is None or args.token is None or args.port is None:
            parser.error("serve requires --directory, --token and --port")
        serve(args.state_dir.resolve(), args.directory.resolve(), args.token, args.port)
    else:
        try:
            (start if args.action == "start" else stop)(args.state_dir.resolve())
        except (OSError, ValueError, RuntimeError) as error:
            parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
