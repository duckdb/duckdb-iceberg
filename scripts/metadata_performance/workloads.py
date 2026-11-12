"""Shape-checked SQL workloads for #1500, #1654 and #1721; no catalog configuration."""

import datetime as dt
from dataclasses import dataclass


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class DeleteShape:
    months: int
    batches: int
    rows_per_file: int
    deletes: int
    batches_per_delete: int


SHAPES = {
    "reduced": DeleteShape(4, 4, 28, 1, 1),
    "regression": DeleteShape(20, 12, 100, 2, 2),
    "full": DeleteShape(20, 150, 1000, 10, 4),
}


def row_key(batch, row):
    return f"b{batch:05d}-r{row:06d}"


def month_start(month):
    return dt.date(2025 + month // 12, month % 12 + 1, 1)


def delete_setup(shape, table, with_deletes=True):
    yield f"""CREATE TABLE {table} (k VARCHAR, d DATE, v BIGINT)
        PARTITIONED BY (month(d)) WITH (
        'format-version'='3', 'commit.manifest-merge.enabled'='false')"""
    for batch in range(shape.batches):
        yield f"""INSERT INTO {table}
            SELECT printf('b%05d-r%06d', {batch}, i),
                (DATE '2025-01-01' + to_months((i % {shape.months})::INTEGER))::DATE
                    + ((i // {shape.months} + {batch}) % 28)::INTEGER, i
            FROM range({shape.months * shape.rows_per_file}) t(i)"""
    if with_deletes:
        for delete in range(shape.deletes):
            keys = [
                literal(row_key(batch, month))
                for batch in range(delete * shape.batches_per_delete, (delete + 1) * shape.batches_per_delete)
                for month in range(shape.months)
            ]
            yield f"DELETE FROM {table} WHERE k IN ({','.join(keys)})"


def layout_query(table):
    return f"""SELECT
        count(*) FILTER (WHERE manifest_content='DATA' AND status<>'DELETED') AS data_files,
        coalesce(sum(record_count) FILTER (WHERE manifest_content='DATA' AND status<>'DELETED'),0)::BIGINT AS data_rows,
        count(*) FILTER (WHERE manifest_content='DELETE' AND status<>'DELETED') AS live_delete_entries,
        count(DISTINCT manifest_path) FILTER (WHERE manifest_content='DATA') AS data_manifests,
        count(DISTINCT manifest_path) FILTER (WHERE manifest_content='DELETE') AS delete_manifests,
        count(DISTINCT file_path) FILTER (WHERE manifest_content='DELETE' AND status<>'DELETED') AS delete_files
        FROM iceberg_metadata({literal(table)})"""


def expected_layout(shape, with_deletes=True):
    deletes = shape.deletes if with_deletes else 0
    entries = deletes * shape.batches_per_delete * shape.months
    return {
        "data_files": shape.months * shape.batches,
        "data_rows": shape.months * shape.batches * shape.rows_per_file,
        "live_delete_entries": entries,
        "data_manifests": shape.batches,
        "delete_manifests": deletes,
        # Current writer packs all vectors from one DELETE into one Puffin file.
        "delete_files": deletes,
    }


def delete_cases(shape, table, with_deletes=True):
    """Keep literal IN lists and a semi join, as in the issue's collapsed repro.py."""
    for scenario in ("wide", "narrow"):
        months = range(shape.months) if scenario == "wide" else range(max(0, shape.months - 4), shape.months)
        days = range(28) if scenario == "wide" else range(0, 22, 2)
        dates, keys = [], []
        for month in months:
            for day in days:
                batch = (month * 28 + day) % shape.batches
                row = month + shape.months * ((day - batch) % 28)
                dates.append(month_start(month) + dt.timedelta(days=day))
                keys.append(row_key(batch, row))
        variants = ["in", "between", "none"] if scenario == "wide" else ["in", "padded_in", "between"]
        for variant in variants:
            selected_dates = dates
            if variant == "padded_in":
                selected_dates = dates + [dt.date(1800, 1, 1) + dt.timedelta(days=i) for i in range(550 - len(dates))]
            if variant in ("in", "padded_in"):
                predicate = "d IN (" + ",".join(f"DATE {literal(d)}" for d in selected_dates) + ")"
            elif variant == "between":
                predicate = f"d BETWEEN DATE {literal(dates[0])} AND DATE {literal(dates[-1])}"
            else:
                predicate = "true"
            # Independent row oracle: decode each key, derive its actual stored date,
            # account for deletes, then evaluate each variant separately.
            expected = 0
            for key in set(keys):
                batch, row = int(key[1:6]), int(key[8:])
                actual_date = month_start(row % shape.months) + dt.timedelta(days=(row // shape.months + batch) % 28)
                deleted = with_deletes and batch < shape.deletes * shape.batches_per_delete and row < shape.months
                matches = (
                    actual_date in selected_dates
                    if variant in ("in", "padded_in")
                    else dates[0] <= actual_date <= dates[-1] if variant == "between" else True
                )
                expected += int(row < shape.months * shape.rows_per_file and not deleted and matches)
            yield {
                "name": f"{scenario}_{variant}" + ("" if with_deletes else "_no_deletes"),
                "keys_sql": "CREATE OR REPLACE TEMP TABLE keys AS SELECT unnest(["
                + ",".join(literal(key) for key in keys)
                + "]) AS k",
                "sql": f"SELECT count(*) AS result FROM {table} WHERE k IN (SELECT k FROM keys) AND {predicate}",
                "expected": expected,
                "constants": (
                    len(selected_dates) if variant in ("in", "padded_in") else 2 if variant == "between" else 0
                ),
            }


def manifest_setup(entries, table):
    columns = ",".join(f"c{i} VARCHAR" for i in range(16))
    values = ",".join(f"md5(range::VARCHAR || '{i}')" for i in range(16))
    yield f"CREATE TABLE {table} (k VARCHAR, ts TIMESTAMP, {columns}) PARTITIONED BY (k, day(ts))"
    yield f"INSERT INTO {table} SELECT 'k' || range, TIMESTAMP '2026-01-01', {values} FROM range({entries})"


def manifest_case(table):
    return f"""SELECT count(*) AS result FROM {table} WHERE k='k7'
        AND ts >= TIMESTAMP '2026-01-01' AND ts < TIMESTAMP '2026-01-02'"""


def wide_manifest_setup(entries, table):
    # #1500: one manifest, 101 statistics-bearing columns, 100 rows per file.
    columns = ",".join(f"c{i} BIGINT" for i in range(1, 101))
    values = ",".join(f"range+{i}" for i in range(1, 101))
    yield f"""CREATE TABLE {table} (p INTEGER, {columns}) PARTITIONED BY (p)
        WITH ('commit.manifest-merge.enabled'='false')"""
    yield f"INSERT INTO {table} SELECT range % {entries}, {values} FROM range({entries * 100})"


def wide_manifest_case(table):
    # c1 is not a partition column: the manifest survives pruning, while each
    # data file's lower bound excludes it. No Parquet file needs to be read.
    return f"SELECT count(*) AS result FROM {table} WHERE c1 < 0"
