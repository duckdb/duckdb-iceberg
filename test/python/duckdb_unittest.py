import os
import subprocess
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEST_CONFIG = REPO_ROOT / "test" / "configs" / "fixture.json"

STANDARD_PREAMBLE = """
require-env CATALOG_TEST_CONFIG_SETUP

require avro

require parquet

require iceberg

require httpfs

require core_functions

# Do not ignore 'HTTP' error messages!
set ignore_error_messages

statement ok
set enable_logging=true

statement ok
set logging_level='debug'
"""

ALL_TESTS_SKIPPED_MARKER = "All tests were skipped"


class DuckDBUnittestRunner:
    def __init__(
        self,
        unittest_binary: str,
        *,
        test_config: Path | str = DEFAULT_TEST_CONFIG,
        env: dict[str, str] | None = None,
        print_stdin: bool = False,
        preamble: str | None = STANDARD_PREAMBLE,
    ) -> None:
        self.unittest_binary = unittest_binary
        self.test_config = Path(test_config)
        self.env = env
        self.print_stdin = print_stdin
        self.preamble = preamble
        self.process: subprocess.Popen[str] | None = None
        self.stdin_log: list[str] = []
        self._final_result: tuple[str, str, int] | None = None

    def __enter__(self):
        self.process = subprocess.Popen(
            [
                self.unittest_binary,
                "--stdin",
                "--test-config",
                str(self.test_config),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, **(self.env or {})},
        )
        if self.preamble is not None:
            self.send(self.preamble)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_type is not None:
            stdout, stderr, returncode = self._terminate()
            if exc_value is not None and (returncode != 0 or stderr):
                exc_value.add_note(self._failure_message(stdout, stderr, returncode))
            return False

        stdout, stderr, returncode = self._finish()
        if returncode != 0 or self._all_tests_skipped(stdout, stderr):
            raise AssertionError(self._failure_message(stdout, stderr, returncode))
        return False

    def _active_process(self) -> subprocess.Popen[str]:
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("DuckDBUnittestRunner is not active")
        return self.process

    def send(self, text: str) -> None:
        block = textwrap.dedent(text).strip()
        if block:
            self.send_raw(block)

    def send_raw(self, text: str) -> None:
        process = self._active_process()
        if not text.strip():
            return
        self.stdin_log.append(text)
        process.stdin.write(text)
        if not text.endswith("\n"):
            process.stdin.write("\n")
        process.stdin.write("\n")
        process.stdin.flush()

    def run_sqllogic_file(self, path: Path | str) -> None:
        self.send_raw(Path(path).read_text())

    @property
    def stdin_text(self) -> str:
        if not self.stdin_log:
            return ""
        return "\n\n".join(self.stdin_log) + "\n\n"

    @staticmethod
    def _header(prefix: str, connection: str | None = None) -> str:
        if connection is None:
            return prefix
        return f"{prefix} {connection}"

    def statement_ok(self, sql: str, *, connection: str | None = None) -> None:
        self.send(f"{self._header('statement ok', connection)}\n{textwrap.dedent(sql).strip()}")

    def statement_error(self, sql: str, expected_error: str | None = None, *, connection: str | None = None) -> None:
        block = f"{self._header('statement error', connection)}\n{textwrap.dedent(sql).strip()}"
        if expected_error is not None:
            block += f"\n----\n{textwrap.dedent(expected_error).strip()}"
        self.send(block)

    @contextmanager
    def with_transaction(self, commit: bool = True, *, connection: str | None = None) -> Iterator[None]:
        self.statement_ok("begin transaction", connection=connection)
        try:
            yield
        except BaseException:
            self.statement_ok("abort", connection=connection)
            raise
        else:
            self.statement_ok("commit" if commit else "abort", connection=connection)

    def query(
        self,
        column_types: str,
        sql: str,
        expected_rows: Sequence[Sequence[object]],
        *,
        connection: str | None = None,
    ) -> None:
        rows = []
        for row in expected_rows:
            if isinstance(row, (str, bytes)) or len(row) != len(column_types):
                raise ValueError(f"Expected {len(column_types)} values for query type '{column_types}', got {len(row)}")
            rows.append("\t".join(self._format_value(value) for value in row))

        expected = "\n".join(rows)
        self.send(
            f"{self._header(f'query {column_types}', connection)}\n{textwrap.dedent(sql).strip()}\n----\n{expected}"
        )

    @staticmethod
    def _format_value(value: object) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    def _finish(self) -> tuple[str, str, int]:
        if self._final_result is not None:
            return self._final_result

        process = self._active_process()
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        process.stdin = None
        stdout, stderr = process.communicate()
        if self.print_stdin:
            print(f"DuckDBUnittestRunner stdin:\n{self.stdin_text}", end="")
        self._final_result = (stdout, stderr, process.returncode)
        return self._final_result

    def _failure_message(self, stdout: str, stderr: str, returncode: int) -> str:
        skipped_suffix = ""
        if self._all_tests_skipped(stdout, stderr):
            skipped_suffix = " (all tests were skipped)"
        return (
            f"stdin unittest exited with code {returncode}{skipped_suffix}\n"
            f"stdin:\n{self.stdin_text}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    @staticmethod
    def _all_tests_skipped(stdout: str, stderr: str) -> bool:
        return ALL_TESTS_SKIPPED_MARKER in stdout or ALL_TESTS_SKIPPED_MARKER in stderr

    def _terminate(self) -> tuple[str, str, int]:
        if self.process is None:
            return "", "", 0
        if self._final_result is not None:
            return self._final_result
        if self.process.poll() is None:
            self.process.kill()
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        self.process.stdin = None
        stdout, stderr = self.process.communicate()
        self._final_result = (stdout, stderr, self.process.returncode)

        for stream_name in ("stdin", "stdout", "stderr"):
            stream = getattr(self.process, stream_name, None)
            if stream is not None:
                stream.close()
        return self._final_result
