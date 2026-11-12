#!/usr/bin/env python3
"""Compare partition candidate indexes using unreferenced v2 position deletes."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics

import fastavro

from run import CONFIG, ROOT, Shell, git, payload, response
from workloads import literal


def rewrite_avro(path, mutate, output):
    # Only edit this benchmark's newly generated, private files before publishing cases.json.
    path = Path(path)
    assert path.resolve().is_relative_to(output.resolve()), path
    with path.open("rb") as source:
        reader = fastavro.reader(source)
        records = list(reader)
        schema, codec = reader.writer_schema, reader.codec
        metadata = {k: v for k, v in reader.metadata.items() if k not in ("avro.schema", "avro.codec")}
    for record in records:
        mutate(record)
    with path.open("wb") as target:
        fastavro.writer(target, schema, records, codec=codec, metadata=metadata)
    return path.stat().st_size


def prepare(shell, partitions):
    name = f"partition_index_{partitions}"
    table = f"my_datalake.default.{name}"
    shell.query("SET threads=1")
    shell.query(
        f"""CREATE TABLE {table} (p INTEGER, q VARCHAR, id BIGINT)
        PARTITIONED BY (p, q) WITH
        ('format-version'=2, 'write.delete.mode'='merge-on-read',
         'commit.manifest-merge.enabled'='false')"""
    )
    # Four files per composite partition, including a NULL first component.
    for _ in range(4):
        shell.query(
            f"""INSERT INTO {table}
            SELECT CASE WHEN i%{partitions}%11=0 THEN NULL ELSE i%{partitions} END,
                   printf('partition-%05d', i%{partitions}), i//{partitions}
            FROM range({partitions * 16}) t(i)"""
        )
    shell.query(f"DELETE FROM {table} WHERE id=0")
    shell.query("BEGIN")
    try:
        layout = shell.query(
            f"""SELECT count(*) FILTER (WHERE content='DATA') AS data_files,
                       count(*) FILTER (WHERE content='POSITION_DELETES') AS delete_entries
                FROM iceberg_metadata({table}) WHERE status<>'DELETED'"""
        )[0]
        assert layout == {"data_files": partitions * 4, "delete_entries": partitions * 4}, layout
        paths = shell.query(
            f"""SELECT DISTINCT manifest_path FROM iceberg_metadata({table})
                WHERE content='POSITION_DELETES' AND status<>'DELETED'"""
        )
        assert len(paths) == 1, paths
        # Our writer includes this optional field even in v2. Remove it from
        # private fixtures to exercise valid unreferenced position deletes.
        manifest = paths[0]["manifest_path"]
        length = rewrite_avro(manifest, lambda row: row["data_file"].update(referenced_data_file=None), shell.output)
        table_payload = payload(shell, name)
        metadata = table_payload["metadata"]
        snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
        rewrite_avro(
            snapshot["manifest-list"],
            lambda row: row.update(manifest_length=length) if row["manifest_path"] == manifest else None,
            shell.output,
        )
        shell.query("SET enable_external_file_cache=false")
        unreferenced = shell.query(
            f"""SELECT count(*) AS entries,
                       count(json_extract_string(to_json(data_file), '$.referenced_data_file')) AS references
                FROM read_avro({literal(paths[0]['manifest_path'])})"""
        )[0]
        assert unreferenced == {"entries": partitions * 4, "references": 0}, unreferenced
    finally:
        shell.query("COMMIT")
    return {
        "name": name,
        "partitions": partitions,
        "layout": layout,
        "payload": table_payload,
        "sql": f"SELECT sum(id+1)::BIGINT AS result FROM {table} WHERE id>=0",
        "expected": partitions * 4 * sum(range(2, 17)),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=ROOT / "build/reldebug")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, help="Reuse a previous run's immutable datasets and cases.json")
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    args.build = args.build.resolve()
    args.output = args.output.resolve()
    cache = (args.build / "CMakeCache.txt").read_text()
    if re.search(r"^COVERAGE:BOOL=(ON|TRUE|1)$", cache, re.MULTILINE):
        parser.error("Build with COVERAGE=OFF")
    args.output.mkdir(parents=True, exist_ok=False)
    setup = Shell(args.build, args.output)
    try:
        versions = setup.query("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded")
        loaded = {row["extension_name"]: row["extension_version"] for row in versions}
        assert loaded["iceberg"] == git("rev-parse", "HEAD")[:10], loaded
        assert loaded["core_functions"] == git("-C", "duckdb", "rev-parse", "HEAD")[:10], loaded
        environment = {
            "extension_revision": git("rev-parse", "HEAD"),
            "core_revision": git("-C", "duckdb", "rev-parse", "HEAD"),
            "diff": git("diff"),
            "versions": versions,
            "cmake_cache": cache,
            "config": str(CONFIG),
            "config_sha256": hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
            "cli_sha256": hashlib.sha256((args.build / "duckdb").read_bytes()).hexdigest(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        }
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
        if args.source:
            cases = json.loads((args.source / "cases.json").read_text())
        else:
            cases = [prepare(setup, partitions) for partitions in (8, 64, 512)]
        (args.output / "cases.json").write_text(json.dumps(cases, indent=2) + "\n")
        samples = []
        for case in cases:
            for threads in (1, 8):
                for mode in ("cold", "warm"):
                    label = f"{case['name']}-t{threads}-{mode}"
                    shell = None
                    try:
                        for sample in range(args.samples):
                            if shell is None:
                                shell = Shell(args.build, args.output)
                                response(shell, case["name"], case["payload"])
                                shell.query(f"SET threads={threads}")
                                shell.query(f"SET enable_external_file_cache={'false' if mode == 'cold' else 'true'}")
                                if mode == "warm":
                                    assert shell.query(case["sql"]) == [{"result": case["expected"]}]
                            result, seconds, cpu = shell.timed(
                                case["sql"], args.output / f"{label}-{sample}.profile.json"
                            )
                            assert result == [{"result": case["expected"]}], (case, result)
                            samples.append(
                                dict(
                                    partitions=case["partitions"],
                                    threads=threads,
                                    mode=mode,
                                    sample=sample,
                                    seconds=seconds,
                                    cpu_seconds=cpu,
                                    result=result,
                                )
                            )
                            if mode == "cold" and sample + 1 < args.samples:
                                shell.close()
                                shell = None
                        shell.query("CALL enable_logging('Iceberg', storage='memory')")
                        assert shell.query(case["sql"]) == [{"result": case["expected"]}]
                        logs = shell.query("SELECT message FROM duckdb_logs() WHERE type='Iceberg'")
                        counts = {
                            key: sum(int(v) for row in logs for v in re.findall(key + r"=(\d+)", row["message"]))
                            for key in (
                                "delete_index_builds",
                                "delete_candidate_visits",
                                "delete_predicate_evaluations",
                            )
                        }
                        assert counts == {
                            "delete_index_builds": 1,
                            "delete_candidate_visits": case["partitions"] * 16,
                            "delete_predicate_evaluations": case["partitions"] * 4,
                        }, counts
                        (args.output / f"{label}.logs.json").write_text(json.dumps(logs, indent=2) + "\n")
                        (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                        median = statistics.median(
                            row["seconds"] * 1000
                            for row in samples
                            if row["partitions"] == case["partitions"]
                            and row["threads"] == threads
                            and row["mode"] == mode
                        )
                        print(f"{label}: {median:.3f} ms", flush=True)
                    finally:
                        if shell:
                            shell.close()
    finally:
        setup.close()


if __name__ == "__main__":
    main()
