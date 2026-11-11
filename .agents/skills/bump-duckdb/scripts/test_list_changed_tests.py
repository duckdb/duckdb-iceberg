"""Exercise test selection using actual Git history, without a DuckDB build."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from list_changed_tests import list_changed_tests


class ChangedTestsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        for name in ("modified", "unchanged", "deleted", "renamed", "type_changed"):
            self.write(f"test/sql/{name}.test", f"# {name}\nstatement ok\nSELECT 1;\n")
        self.write("test/other/outside.test", "# outside SQL directory\n")
        self.start = self.commit()

        self.write("test/sql/modified.test", "# modified\nstatement ok\nSELECT 2;\n")
        self.write("test/sql/added.test", "# new\n")
        self.write("test/sql/space in name.test", "# whitespace\n")
        self.write("test/sql/slow.test_slow", "# slow\n")
        self.write("test/sql/coverage.test_coverage", "# coverage\n")
        self.write("test/sql/helper.py", "# helper\n")
        self.write("test/other/outside.test", "# changed outside SQL directory\n")
        (self.repo / "test/sql/deleted.test").unlink()
        self.git("mv", "test/sql/renamed.test", "test/sql/new_name.test")
        (self.repo / "test/sql/type_changed.test").unlink()
        (self.repo / "test/sql/type_changed.test").symlink_to("unchanged.test")
        self.end = self.commit()
        # Neither staged nor unstaged changes belong to a committed bump.
        self.write("test/sql/uncommitted.test", "# untracked\n")
        self.write("test/sql/unchanged.test", "# changed only in working tree\n")
        self.git("add", "test/sql/unchanged.test")

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def write(self, path, content):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self):
        self.git("add", ".")
        self.git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "test revision")
        return self.git("rev-parse", "HEAD")

    def test_default_matches_fixture_ci(self):
        self.assertEqual(
            list_changed_tests(self.repo, self.start, self.end),
            [
                "test/sql/added.test",
                "test/sql/coverage.test_coverage",
                "test/sql/modified.test",
                "test/sql/new_name.test",
                "test/sql/space in name.test",
                "test/sql/type_changed.test",
            ],
        )

    def test_slow_is_opt_in(self):
        paths = list_changed_tests(self.repo, self.start, self.end, include_slow=True)
        self.assertIn("test/sql/slow.test_slow", paths)
        self.assertEqual(len(paths), 7)

    def test_pathspec_and_subdirectory_keep_repo_relative_paths(self):
        self.assertEqual(
            list_changed_tests(self.repo / "test", self.start, self.end, "test/other"),
            ["test/other/outside.test"],
        )

    def test_unchanged_revisions_produce_empty_list(self):
        self.assertEqual(list_changed_tests(self.repo, self.end, self.end), [])

    def test_bad_revision_is_an_error(self):
        with self.assertRaisesRegex(RuntimeError, "Git command failed"):
            list_changed_tests(self.repo, "missing-revision", self.end)

    def test_newline_path_is_rejected(self):
        self.write("test/sql/line\nbreak.test", "# unrepresentable\n")
        end = self.commit()
        with self.assertRaisesRegex(ValueError, "line-based"):
            list_changed_tests(self.repo, self.start, end)

    def test_cli_prints_only_paths_and_reports_invalid_revisions(self):
        command = [
            sys.executable,
            str(Path(__file__).with_name("list_changed_tests.py")),
            str(self.repo),
            self.start,
            self.end,
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            list_changed_tests(self.repo, self.start, self.end),
        )
        command[-2] = "missing-revision"
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("Git command failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
