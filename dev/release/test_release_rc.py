#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import gzip
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


MOCK_TOOL = r'''
import os
from pathlib import Path
import shutil
import subprocess
import sys

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
if tool in ("git", "gzip"):
    result = subprocess.run([os.environ["REAL_" + tool.upper()], *args])
    if tool == "git" and "archive" in args and os.environ.get("FAIL_ARCHIVE"):
        sys.exit(1)
    sys.exit(result.returncode)
elif tool == "gh" and args[:2] == ["run", "watch"]:
    pass
elif tool == "gh" and args[:2] == ["release", "download"]:
    destination = Path(args[args.index("--dir") + 1])
    for index, arg in enumerate(args):
        if arg == "--pattern":
            filename = args[index + 1]
            target = destination / filename
            if target.exists() and "--skip-existing" in args:
                continue
            shutil.copyfile(Path(os.environ["ASSETS"]) / filename, target)
elif tool == "gpg":
    Path(args[args.index("--output") + 1]).write_text("test signature")
elif tool == "svn" and args[0] == "import":
    pass
elif tool != "gh" or args[:2] != ["release", "upload"]:
    sys.exit("Unexpected command: " + tool + " " + " ".join(args))

if tool in ("gpg", "svn") or (tool == "gh" and args[:2] == ["release", "upload"]):
    with open(os.environ["CALL_LOG"], "a") as log:
        log.write(tool + " " + " ".join(args) + "\n")
'''


class ReleaseRcTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="iceberg-release-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.repo = self.directory / "repo"
        self.repo.mkdir()
        self.assets = self.directory / "assets"
        self.assets.mkdir()
        self.calls = self.directory / "calls.log"
        self.scratch = self.directory / "scratch"
        self.scratch.mkdir()
        self.name = "apache-iceberg-cpp-1.2.3"
        self.tar_gz = self.name + ".tar.gz"
        self.output = self.repo / (self.name + "-rc0")
        self.real_git = shutil.which("git")
        self.env = os.environ.copy()
        for key in list(self.env):
            if key.startswith(("RELEASE_", "GIT_")):
                del self.env[key]
        self.env.update({
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Release Test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "Release Test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
            "RELEASE_DEFAULT": "0",
            "RELEASE_SIGN": "1",
            "RELEASE_UPLOAD": "1",
            "RELEASE_WATCH": "1",
            "RELEASE_RUN_ID": "123",
            "ASSETS": str(self.assets),
            "CALL_LOG": str(self.calls),
            "REAL_GIT": self.real_git,
            "REAL_GZIP": shutil.which("gzip"),
            "TMPDIR": str(self.scratch),
        })
        self.git("init", "--quiet")
        self.git("remote", "add", "origin", "https://github.com/example/iceberg-cpp.git")
        (self.repo / "source.txt").write_text("original source\n")
        self.git("add", "source.txt")
        self.git("commit", "--quiet", "-m", "Initial source")
        self.git("tag", "-a", "v1.2.3-rc0", "-m", "Release candidate")
        self.archive = self.git("archive", "HEAD", "--prefix", self.name + "/")
        self.publish(gzip.compress(self.archive, mtime=123))

        script = Path(__file__).with_name("release_rc.sh")
        self.script = self.repo / "dev" / "release" / script.name
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(script, self.script)
        commands = self.directory / "bin"
        commands.mkdir()
        for tool in ("gh", "gpg", "svn", "git", "gzip"):
            executable = commands / tool
            executable.write_text("#!" + sys.executable + "\n" + MOCK_TOOL)
            executable.chmod(0o755)
        self.env["PATH"] = str(commands) + os.pathsep + self.env["PATH"]

    def git(self, *args):
        return subprocess.check_output(
            [self.real_git, *args], cwd=self.repo, env=self.env,
            stderr=subprocess.STDOUT,
        )

    def publish(self, archive):
        (self.assets / self.tar_gz).write_bytes(archive)
        checksum = hashlib.sha512(archive).hexdigest()
        (self.assets / (self.tar_gz + ".sha512")).write_text(
            checksum + "  " + self.tar_gz + "\n"
        )

    def release(self):
        return subprocess.run(
            ["bash", str(self.script), "1.2.3", "0"],
            cwd=self.repo, env=self.env, capture_output=True, text=True,
            timeout=30,
        )

    def assert_rejected(self, result, message):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(message, result.stdout + result.stderr)
        self.assertFalse(self.calls.exists(), "Signing or upload was attempted")
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_valid_archive_with_different_gzip_metadata(self):
        result = self.release()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            (self.output / self.tar_gz).read_bytes(),
            (self.assets / self.tar_gz).read_bytes(),
        )
        self.assertEqual(
            {path.name for path in self.output.iterdir()},
            {self.tar_gz, self.tar_gz + ".sha512", self.tar_gz + ".asc"},
        )
        calls = self.calls.read_text().splitlines()
        self.assertEqual([line.split()[0] for line in calls], ["gpg", "gh", "svn"])
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_replaced_archive_and_checksum(self):
        (self.repo / "source.txt").write_text("modified source\n")
        self.git("add", "source.txt")
        self.git("commit", "--quiet", "-m", "Unreleased changes")
        modified = self.git("archive", "HEAD", "--prefix", self.name + "/")
        self.publish(gzip.compress(modified))
        self.assert_rejected(self.release(), "does not match RC commit")

    def test_incorrect_checksum(self):
        (self.assets / (self.tar_gz + ".sha512")).write_text(
            "0" * 128 + "  " + self.tar_gz + "\n"
        )
        self.assert_rejected(self.release(), "failed SHA-512 verification")

    def test_corrupt_gzip_trailer_with_matching_checksum(self):
        archive = bytearray((self.assets / self.tar_gz).read_bytes())
        archive[-8] ^= 1
        self.publish(archive)
        self.assert_rejected(self.release(), "Failed to decompress")

    def test_existing_release_directory_is_preserved(self):
        self.output.mkdir()
        stale = self.output / self.tar_gz
        stale.write_bytes(b"stale archive")
        self.assert_rejected(self.release(), "already exists")
        self.assertEqual(stale.read_bytes(), b"stale archive")

    def test_failed_git_archive_after_writing_complete_output(self):
        self.env["FAIL_ARCHIVE"] = "1"
        self.assert_rejected(self.release(), "Failed to archive RC commit")


if __name__ == "__main__":
    unittest.main()
