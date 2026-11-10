#!/usr/bin/env python3
"""List changed DuckDB SQL tests as input for unittest --input-file."""

import argparse
import subprocess
from pathlib import Path


def _git(repo: Path, *args: str) -> str:
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError as exc:
        raise RuntimeError("Git is not installed or is not available on PATH.") from exc
    except subprocess.CalledProcessError as exc:
        error = exc.stderr.strip() or "Unknown Git error"
        raise RuntimeError(f"Git command failed: {error}") from exc


def list_changed_tests(
    repo_path: str | Path,
    start_hash: str,
    end_hash: str,
    pathspec: str = "test/sql/",
    *,
    include_slow: bool = False,
) -> list[str]:
    """Return added/modified/renamed/type-changed test paths at the end revision.

    Compare two commits directly, ignoring uncommitted changes. The default
    suffixes match Iceberg's fixture CI selection, which excludes .test_slow.
    Paths are relative to the Git repository root, not the caller's directory.
    """
    repo = Path(repo_path).expanduser().resolve()
    if not repo.is_dir():
        raise ValueError(f"Repository directory does not exist: {repo}")
    repo = Path(_git(repo, "rev-parse", "--show-toplevel").strip())
    start = _git(
        repo, "rev-parse", "--verify", "--end-of-options", f"{start_hash}^{{commit}}"
    ).strip()
    end = _git(
        repo, "rev-parse", "--verify", "--end-of-options", f"{end_hash}^{{commit}}"
    ).strip()
    output = _git(
        repo,
        "diff",
        "--no-ext-diff",
        "--name-only",
        "--diff-filter=AMRT",
        "--find-renames",
        "-z",
        start,
        end,
        "--",
        pathspec,
    )
    suffixes = (".test", ".test_coverage")
    if include_slow:
        suffixes += (".test_slow",)
    paths = sorted({path for path in output.split("\0") if path.endswith(suffixes)})
    for path in paths:
        if "\n" in path or "\r" in path:
            raise ValueError(
                f"Test path cannot be represented in a line-based input file: {path!r}"
            )
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List added and changed DuckDB SQL tests between two Git revisions.",
        epilog="Output is one repository-relative path per line for unittest --input-file.",
    )
    parser.add_argument("repo_path", help="Path to the cloned DuckDB repository")
    parser.add_argument("start_hash", help="Starting commit, branch, or tag")
    parser.add_argument("end_hash", help="Ending commit, branch, or tag")
    parser.add_argument(
        "pathspec",
        nargs="?",
        default="test/sql/",
        help='Optional directory or Git pathspec (default: "test/sql/")',
    )
    parser.add_argument(
        "--include-slow",
        action="store_true",
        help="Include .test_slow as well as .test and .test_coverage",
    )
    args = parser.parse_args()
    try:
        paths = list_changed_tests(
            args.repo_path,
            args.start_hash,
            args.end_hash,
            args.pathspec,
            include_slow=args.include_slow,
        )
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
