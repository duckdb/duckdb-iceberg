#!/usr/bin/env python3
"""Run metadata baselines with the built CLI and native REST test provider (stdlib only)."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import time

from workloads import (
    SHAPES,
    delete_cases,
    delete_setup,
    expected_layout,
    layout_query,
    literal,
    manifest_case,
    manifest_setup,
)


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "test/configs/mock_native.json"


class Shell:
    """One CLI process = one fresh in-memory DatabaseInstance and one connection."""

    def __init__(self, build, output, staleness=False):
        self.output = output
        self.stderr = (output / f"shell-{time.time_ns()}.stderr").open("w+")
        self.process = subprocess.Popen(
            [str(build / "duckdb"), "-unsigned", "-no-agent", "-batch", "-init", "/dev/null"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
            bufsize=1,
            cwd=ROOT,
        )
        self.raw(".bail on\n.mode json")
        config = json.loads(CONFIG.read_text())
        for extension in config["statically_loaded_extensions"]:
            path = build / "extension" / extension / f"{extension}.duckdb_extension"
            self.query(f"LOAD {literal(path)}")
        init = config["on_init"].replace("{TEMP_DIR}", str(output))
        if staleness:
            init = init.replace("STAGE_CREATE_TABLES true", "STAGE_CREATE_TABLES true, MAX_TABLE_STALENESS '1 hour'")
        self.query(init)
        self.query("SET iceberg_use_server_side_scan_planning=false")

    def raw(self, text):
        marker = "__metadata_baseline_end__"
        self.process.stdin.write(text + f"\n.print {marker}\n")
        self.process.stdin.flush()
        lines = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                self.stderr.flush()
                self.stderr.seek(0)
                raise RuntimeError(f"DuckDB exited during:\n{text[:2000]}\n{self.stderr.read()}")
            if line.strip() == marker:
                return "".join(lines)
            lines.append(line)

    def query(self, sql):
        text = self.raw(sql.rstrip(";") + ";")
        if text.startswith("WARNING:\n") and "\n[" in text:
            warning, text = text.split("\n[", 1)
            with (self.output / "cli-warnings.txt").open("a") as out:
                out.write(warning + "\n")
            text = "[" + text
        try:
            return json.loads(text) if text.strip() else []
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Unexpected CLI output for {sql[:2000]}:\n{text}") from error

    def timed(self, sql, profile):
        self.query("SET enable_profiling='json'")
        self.query(f"SET profiling_output={literal(profile)}")
        start = time.perf_counter()
        text = self.raw(".timer on\n" + sql.rstrip(";") + ";\n.timer off")
        elapsed = time.perf_counter() - start
        self.query("PRAGMA disable_profiling")
        timing = re.search(r"Run Time \(s\): real ([\d.]+) user ([\d.]+) sys ([\d.]+)", text)
        if not timing:
            raise RuntimeError(f"Missing CLI CPU timing: {text}")
        result = json.loads(text[: timing.start()])
        return result, elapsed, float(timing[2]) + float(timing[3])

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            self.process.wait(timeout=30)
        self.stderr.close()
        if self.process.returncode:
            raise RuntimeError(f"DuckDB exited with {self.process.returncode}")


def response(shell, name, payload, status=200):
    request = {
        "method": "GET",
        "path": f"/v1/namespaces/default/tables/{name}",
        "status": status,
        "body": payload,
        "headers": {"ETag": '"metadata-baseline"'},
    }
    result = shell.query(
        "SELECT status,body FROM iceberg_test_catalog_request('POST','/__test/response',"
        + literal(json.dumps(request, separators=(",", ":")))
        + ")"
    )
    assert result[0]["status"] == 204, result


def payload(shell, name):
    rows = shell.query(
        f"""SELECT body FROM iceberg_test_catalog_request(
        'GET','/v1/namespaces/default/tables/{name}',NULL)"""
    )
    return json.loads(rows[0]["body"])


def check_layout(shell, table, expected):
    # Path discovery and all direct metadata reads stay in this transaction.
    shell.query("BEGIN")
    try:
        actual = shell.query(layout_query(table))[0]
        assert actual == expected, (table, actual, expected)
    finally:
        shell.query("COMMIT")
    return actual


def scan_nodes(node):
    if isinstance(node, dict):
        if node.get("extra_info", {}).get("Function", "").upper() == "ICEBERG_SCAN":
            yield node
        for key in ("children", "operator"):
            for child in node.get(key, []):
                yield from scan_nodes(child)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=ROOT / "build/reldebug")
    parser.add_argument(
        "--output", type=Path, required=True, help="New directory; contains immutable datasets and raw samples"
    )
    parser.add_argument("--size", choices=SHAPES, default="reduced")
    parser.add_argument("--workload", choices=["all", "delete", "manifest", "snapshots"], default="all")
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 8])
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["cold", "warm", "staleness", "etag"],
        default=["cold", "warm", "staleness", "etag"],
    )
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--case", help="Only measure names containing this string")
    args = parser.parse_args()
    if args.samples < 1 or any(t < 1 for t in args.threads):
        parser.error("samples and threads must be positive")
    args.build = args.build.resolve()
    args.output = args.output.resolve()
    cmake_cache = (args.build / "CMakeCache.txt").read_text()
    if re.search(r"^COVERAGE:BOOL=(ON|TRUE|1)$", cmake_cache, re.MULTILINE):
        parser.error("Rebuild with COVERAGE=OFF before collecting baselines")
    args.output.mkdir(parents=True, exist_ok=False)
    setup = Shell(args.build, args.output)
    try:
        runtime = setup.query("SELECT version() AS core")
        extensions = setup.query("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded")
        revisions = {row["extension_name"]: row["extension_version"] for row in extensions}
        assert revisions["iceberg"] == git("rev-parse", "HEAD")[:10], ("Stale Iceberg build", revisions)
        assert revisions["core_functions"] == git("-C", "duckdb", "rev-parse", "HEAD")[:10], (
            "Stale core build",
            revisions,
        )
        environment = {
            "extension_revision": git("rev-parse", "HEAD"),
            "core_revision": git("-C", "duckdb", "rev-parse", "HEAD"),
            "diff": git("diff"),
            "untracked": git("ls-files", "--others", "--exclude-standard"),
            "runtime": runtime,
            "extensions": extensions,
            "platform": platform.platform(),
            "cpu": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "config": str(CONFIG.relative_to(ROOT)),
            "config_sha256": hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
            "build": str(args.build),
            "cmake_cache": cmake_cache,
            "cli_sha256": hashlib.sha256((args.build / "duckdb").read_bytes()).hexdigest(),
            "harness_sha256": {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob("*.py")
            },
            "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
            "cold_scope": "fresh CLI process, database, connection and attached catalog per sample; OS page cache uncontrolled",
            "warm_scope": "same CLI process, connection, database and attachment for primer and all samples of each case",
        }
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
        setup.query("SET threads=1")
        cases, layouts = [], {}
        shape = SHAPES[args.size]
        if args.workload in ("all", "delete"):
            for with_deletes in (True, False):
                name = "delete_filter" if with_deletes else "delete_control"
                print(f"Preparing {name}: {shape}", flush=True)
                table = f"my_datalake.default.{name}"
                for sql in delete_setup(shape, table, with_deletes):
                    setup.query(sql)
                layouts[name] = check_layout(setup, table, expected_layout(shape, with_deletes))
                live_rows = layouts[name]["data_rows"] - layouts[name]["live_delete_entries"]
                assert setup.query(f"SELECT count(*) AS n FROM {table}") == [{"n": live_rows}]
                for case in delete_cases(shape, table, with_deletes):
                    cases.append({**case, "table": name, "payload": payload(setup, name)})
        if args.workload in ("all", "manifest"):
            sizes = [32] if args.size == "reduced" else [1000, 5000, 25000]
            for entries in sizes:
                name = f"manifest_{entries}"
                print(f"Preparing {name}", flush=True)
                table = f"my_datalake.default.{name}"
                for sql in manifest_setup(entries, table):
                    setup.query(sql)
                layouts[name] = check_layout(
                    setup,
                    table,
                    dict(
                        data_files=entries,
                        data_rows=entries,
                        live_delete_entries=0,
                        data_manifests=1,
                        delete_manifests=0,
                        delete_files=0,
                    ),
                )
                assert setup.query(f"SELECT count(*) AS n FROM {table}") == [{"n": entries}]
                cases.append(
                    dict(name=name, table=name, sql=manifest_case(table), expected=1, payload=payload(setup, name))
                )
        if args.workload in ("all", "snapshots"):
            name = "snapshot_base"
            setup.query(f"CREATE TABLE my_datalake.default.{name} AS SELECT range AS id FROM range(10)")
            original = payload(setup, name)
            sizes = [1, 100] if args.size == "reduced" else [1, 10000, 35000]
            for count in sizes:
                print(f"Preparing snapshots_{count}", flush=True)
                modified = copy.deepcopy(original)
                metadata = modified["metadata"]
                current = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
                metadata["snapshots"] = [
                    {
                        **current,
                        # Keep required summary, without duplicating writer metrics into
                        # 35,000 records (the native request helper has a 16 MiB limit).
                        "summary": {"operation": "append"},
                        "snapshot-id": 10**12 + i,
                        "timestamp-ms": current["timestamp-ms"] - (count - i) * 1000,
                    }
                    for i in range(count - 1)
                ] + [current]
                metadata["snapshot-log"] = [
                    {"snapshot-id": s["snapshot-id"], "timestamp-ms": s["timestamp-ms"]} for s in metadata["snapshots"]
                ]
                assert len(metadata["snapshots"]) == count
                assert len({s["manifest-list"] for s in metadata["snapshots"]}) == 1
                response(setup, name, modified)
                setup.query("BEGIN")
                assert setup.query(f"SELECT count(*) AS n FROM iceberg_snapshots('my_datalake.default.{name}')") == [
                    {"n": count}
                ]
                setup.query("COMMIT")
                layouts[f"snapshots_{count}"] = check_layout(
                    setup,
                    f"my_datalake.default.{name}",
                    dict(
                        data_files=1,
                        data_rows=10,
                        live_delete_entries=0,
                        data_manifests=1,
                        delete_manifests=0,
                        delete_files=0,
                    ),
                )
                cases.append(
                    dict(
                        name=f"snapshots_{count}",
                        table=name,
                        payload=modified,
                        sql=f"SELECT count(*) AS result FROM my_datalake.default.{name} WHERE id=1",
                        expected=1,
                    )
                )
        (args.output / "layouts.json").write_text(json.dumps(layouts, indent=2) + "\n")
        (args.output / "cases.json").write_text(json.dumps(cases, indent=2) + "\n")
        results = []
        for case in cases:
            if args.case and args.case not in case["name"]:
                continue
            for threads in args.threads:
                for mode in args.modes:
                    label = f"{case['name']}-t{threads}-{mode}"
                    shell = None
                    try:
                        for sample in range(args.samples):
                            if shell is None:
                                shell = Shell(args.build, args.output, staleness=mode == "staleness")
                                shell.query(f"SET threads={threads}")
                                shell.query(f"SET enable_external_file_cache={'false' if mode == 'cold' else 'true'}")
                                response(shell, case["table"], case["payload"])
                                if "keys_sql" in case:
                                    shell.query(case["keys_sql"])
                                if mode != "cold":
                                    assert shell.query(case["sql"]) == [{"result": case["expected"]}]
                                if mode == "etag":
                                    response(shell, case["table"], None, status=304)
                                elif mode == "warm":
                                    # Repeated full 200 responses: byte caches may be warm, REST
                                    # revalidation and response parsing still happen on every query.
                                    response(shell, case["table"], case["payload"])
                            profile = args.output / f"{label}-{sample}.profile.json"
                            actual, seconds, cpu_seconds = shell.timed(case["sql"], profile)
                            assert actual == [{"result": case["expected"]}], (case["name"], actual, case["expected"])
                            scans = list(scan_nodes(json.loads(profile.read_text())))
                            # Fail loudly when profiler schema changes; never save fabricated zero counts.
                            assert len(scans) == 1, profile
                            scan = scans[0]
                            files = int(scan["extra_info"]["Total Files Read"])
                            rows = scan.get("operator_rows_scanned", scan.get("rows_scanned"))
                            assert rows is not None, scan
                            results.append(
                                dict(
                                    case=case["name"],
                                    mode=mode,
                                    threads=threads,
                                    sample=sample,
                                    seconds=seconds,
                                    cpu_seconds=cpu_seconds,
                                    files_read=files,
                                    rows_scanned=rows,
                                    result=actual[0]["result"],
                                    profile=profile.name,
                                )
                            )
                            (args.output / "samples.json").write_text(json.dumps(results, indent=2) + "\n")
                            if mode == "cold":
                                shell.close()
                                shell = None
                        # A separate diagnostic query keeps logging out of timing samples.
                        if shell is None:
                            shell = Shell(args.build, args.output)
                            response(shell, case["table"], case["payload"])
                            shell.query(f"SET threads={threads}")
                            shell.query("SET enable_external_file_cache=false")
                            if "keys_sql" in case:
                                shell.query(case["keys_sql"])
                        shell.query("CALL enable_logging(['Iceberg','HTTP'], storage='memory')")
                        shell.query(case["sql"])
                        logs = shell.query("SELECT type,message FROM duckdb_logs() WHERE type IN ('Iceberg','HTTP')")
                        requests = [
                            log["message"]
                            for log in logs
                            if log["type"] == "HTTP" and f"/tables/{case['table']}'" in log["message"]
                        ]
                        if mode == "staleness":
                            assert not requests, requests
                        else:
                            assert len(requests) == 1, requests
                            assert ("NotModified_304" if mode == "etag" else "OK_200") in requests[0], requests
                        (args.output / f"{label}.logs.json").write_text(json.dumps(logs, indent=2) + "\n")
                        values = [
                            r["seconds"] * 1000
                            for r in results
                            if r["case"] == case["name"] and r["threads"] == threads and r["mode"] == mode
                        ]
                        print(f"{label}: {statistics.median(values):.3f} ms", flush=True)
                    finally:
                        if shell:
                            shell.close()
        summary = [
            "| Case | Threads | Mode | Median ms | Median CPU ms | Files | Rows scanned |",
            "|---|---:|---|---:|---:|---:|---:|",
        ]
        groups = sorted({(r["case"], r["threads"], r["mode"]) for r in results})
        for name, threads, mode in groups:
            rows = [r for r in results if (r["case"], r["threads"], r["mode"]) == (name, threads, mode)]
            summary.append(
                f"| {name} | {threads} | {mode} | {statistics.median(r['seconds'] for r in rows)*1000:.3f} | "
                f"{statistics.median(r['cpu_seconds'] for r in rows)*1000:.3f} | "
                f"{sorted({r['files_read'] for r in rows})} | {sorted({r['rows_scanned'] for r in rows})} |"
            )
        (args.output / "summary.md").write_text("\n".join(summary) + "\n")
    finally:
        setup.close()


if __name__ == "__main__":
    main()
