#!/usr/bin/env python3

import argparse
import subprocess
from pathlib import Path

EXAMPLE_USAGE = """
python3 scripts/list_new_files.py duckdb 18ec4b3e4bb22d7fb30e4bb20cc4eaa0a4453c70 531911608719cd03b723051b8cb3b3e81cf3e03b test/sql
"""

def list_new_files(
    repo_path: str | Path,
    start_hash: str,
    end_hash: str,
    pathspec: str = "test/sql/",
) -> list[str]:
    """
    Return repository-relative paths for files added between two Git revisions.

    Args:
        repo_path: Path to the cloned Git repository.
        start_hash: Starting commit, branch, or tag.
        end_hash: Ending commit, branch, or tag.
        pathspec: Optional directory or Git pathspec to filter by.
            Defaults to "test/sql/".

    Returns:
        A sorted list of repository-relative file paths.
    """
    repo = Path(repo_path).expanduser().resolve()

    if not repo.is_dir():
        raise ValueError(f"Repository directory does not exist: {repo}")

    command = [
        "git",
        "-C",
        str(repo),
        "diff",
        "--name-only",
        "--diff-filter=A",
        "-z",
        start_hash,
        end_hash,
        "--",
        pathspec,
    ]

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Git is not installed or is not available on PATH."
        ) from exc
    except subprocess.CalledProcessError as exc:
        error = exc.stderr.strip() or "Unknown Git error"
        raise RuntimeError(f"Git command failed: {error}") from exc

    return sorted(path for path in result.stdout.split("\0") if path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List files added between two Git revisions."
    )
    parser.add_argument(
        "repo_path",
        help="Path to the cloned Git repository",
    )
    parser.add_argument(
        "start_hash",
        help="Starting commit, branch, or tag",
    )
    parser.add_argument(
        "end_hash",
        help="Ending commit, branch, or tag",
    )
    parser.add_argument(
        "pathspec",
        nargs="?",
        default="test/sql/",
        help='Optional directory or Git pathspec (default: "test/sql/")',
    )

    args = parser.parse_args()

    try:
        paths = list_new_files(
            repo_path=args.repo_path,
            start_hash=args.start_hash,
            end_hash=args.end_hash,
            pathspec=args.pathspec,
        )
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))

    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
