#!/usr/bin/env python3

import argparse
import os
import sys
import shutil
import tempfile
import subprocess
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Relativize absolute paths in Iceberg table metadata.")
    parser.add_argument("table_path", help="Path to the Iceberg table (parent directory of 'metadata')")
    return parser.parse_args()


def abort(msg):
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


def relativize_path(abs_path: str, new_base: str) -> str:
    if "/data/" in abs_path:
        return os.path.join(new_base, "data", abs_path.split("/data/", 1)[1])
    elif "/metadata/" in abs_path:
        return os.path.join(new_base, "metadata", abs_path.split("/metadata/", 1)[1])
    else:
        return new_base


def validate_paths(table_path: Path):
    if not table_path.is_dir():
        abort(f"Table path '{table_path}' does not exist or is not a directory")
    if not (table_path / "metadata").is_dir():
        abort(f"'{table_path}/metadata' does not exist. Not an Iceberg table.")

    crc_files = list(table_path.rglob("*.crc"))
    if crc_files:
        print("Found .crc files that may interfere with Avro processing:")
        for f in crc_files:
            print(f"  {f}")
        abort("Please delete .crc files before running this script.")


def jq_rewrite_json(file_path: Path, output_path: Path, new_base: str):
    jq_filter = f'''
        def relativize_path(path; new_base):
            if (path | test(".*/index/data/.*")) then
                (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
            elif (path | test(".*/index/metadata/.*")) then
                (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
            elif (path | test(".*/index$")) then
                new_base
            else
                if (path | test(".*/data/.*")) then
                    (path | sub(".*?/data/"; "")) as $p | new_base + "/data/" + $p
                elif (path | test(".*/metadata/.*")) then
                    (path | sub(".*?/metadata/"; "")) as $p | new_base + "/metadata/" + $p
                else
                    new_base
                end
            end;

        .location = relativize_path(.location; "{new_base}") |
        if .snapshots then
            .snapshots |= map(
                if .["manifest-list"] then
                    .["manifest-list"] = relativize_path(.["manifest-list"]; "{new_base}")
                else . end
            )
        else . end |
        if .["metadata-log"] then
            .["metadata-log"] |= map(
                if .["metadata-file"] then
                    .["metadata-file"] = relativize_path(.["metadata-file"]; "{new_base}")
                else . end
            )
        else . end
    '''
    with open(output_path, 'w') as out:
        returncode = subprocess.run(["jq", jq_filter, str(file_path)], stdout=out, check=True)


def process_json_files(metadata_dir: Path, temp_metadata_dir: Path, new_base: str):
    print("Processing JSON metadata files...")
    for json_file in metadata_dir.glob("*.json"):
        print(f"  Processing {json_file.name}")
        out_file = temp_metadata_dir / json_file.name
        jq_rewrite_json(json_file, out_file, new_base)


def process_avro_file(avro_file: Path, temp_dir: Path, temp_metadata_dir: Path, new_base: str):
    filename = avro_file.name
    schema_path = temp_dir / f"{filename}.avsc"
    json_path = temp_dir / f"{filename}.json"
    processed_json_path = temp_dir / f"{filename}.processed.json"
    output_avro = temp_metadata_dir / filename

    with open(schema_path, 'w') as schema_file:
        subprocess.run(["avro-tools", "getschema", str(avro_file)], stdout=schema_file, check=True)

    with open(json_path, 'w') as json_file:
        subprocess.run(["avro-tools", "tojson", str(avro_file)], stdout=json_file, check=True)

    print(f"  Processing {filename}")
    if filename.startswith("snap-"):
        jq_filter = f'''
            def relativize_path(path; new_base):
                if (path | test(".*/index/data/.*")) then
                    (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
                elif (path | test(".*/index/metadata/.*")) then
                    (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
                elif (path | test(".*/index$")) then
                    new_base
                else
                    if (path | test(".*/data/.*")) then
                        (path | sub(".*?/data/"; "")) as $p | new_base + "/data/" + $p
                    elif (path | test(".*/metadata/.*")) then
                        (path | sub(".*?/metadata/"; "")) as $p | new_base + "/metadata/" + $p
                    else
                        new_base
                    end
                end;

            .manifest_path = relativize_path(.manifest_path; "{new_base}")
        '''
    else:
        jq_filter = f'''
            def relativize_path(path; new_base):
                if (path | test(".*/index/data/.*")) then
                    (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
                elif (path | test(".*/index/metadata/.*")) then
                    (path | sub(".*?/index/"; "")) as $p | new_base + "/" + $p
                elif (path | test(".*/index$")) then
                    new_base
                else
                    if (path | test(".*/data/.*")) then
                        (path | sub(".*?/data/"; "")) as $p | new_base + "/data/" + $p
                    elif (path | test(".*/metadata/.*")) then
                        (path | sub(".*?/metadata/"; "")) as $p | new_base + "/metadata/" + $p
                    else
                        new_base
                    end
                end;

            if .data_file and .data_file.file_path then
                .data_file.file_path = relativize_path(.data_file.file_path; "{new_base}")
            else . end
        '''

    with open(processed_json_path, 'w') as processed:
        subprocess.run(["jq", jq_filter, str(json_path)], stdout=processed, check=True)

    with open(output_avro, 'wb') as out:
        subprocess.run(
            ["avro-tools", "fromjson", "--schema-file", str(schema_path), str(processed_json_path)],
            stdout=out,
            check=True,
        )


def process_avro_files(metadata_dir: Path, temp_dir: Path, temp_metadata_dir: Path, new_base: str):
    print("Processing Avro metadata files...")
    for avro_file in metadata_dir.glob("*.avro"):
        process_avro_file(avro_file, temp_dir, temp_metadata_dir, new_base)


def backup_and_replace_metadata(table_path: Path, temp_metadata_dir: Path):
    backup_dir = table_path / f"metadata.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Creating backup at: {backup_dir}")
    shutil.move(str(table_path / "metadata"), str(backup_dir))
    shutil.move(str(temp_metadata_dir), str(table_path / "metadata"))


def main():
    args = parse_args()
    table_path = Path(args.table_path)
    new_base_path = str(table_path)

    validate_paths(table_path.resolve())

    print(f"Relativizing Iceberg table at: {table_path}")
    print(f"Using new base path: {new_base_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        temp_metadata_dir = temp_dir / "metadata"
        temp_metadata_dir.mkdir(parents=True, exist_ok=True)

        process_json_files(table_path / "metadata", temp_metadata_dir, new_base_path)
        process_avro_files(table_path / "metadata", temp_dir, temp_metadata_dir, new_base_path)

        backup_and_replace_metadata(table_path, temp_metadata_dir)

    print("\nSuccessfully relativized Iceberg table metadata!")
    print("- Rewritten absolute paths to use new base path")
    print("- Updated JSON and Avro metadata files")


if __name__ == "__main__":
    main()
