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

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def extract(pattern: str, relative_path: str) -> str:
    match = re.search(pattern, read(relative_path))
    if match is None:
        raise AssertionError(f"Could not find {pattern!r} in {relative_path}")
    return match.group(1)


class BuildDefinitionParityTest(unittest.TestCase):
    def test_public_headers_are_installed_by_both_build_systems(self) -> None:
        self.assertIn("puffin_dv_io.h", read("src/iceberg/meson.build"))
        self.assertIn(
            "token_refresh_scheduler.h",
            read("src/iceberg/catalog/rest/auth/meson.build"),
        )
        self.assertIn(
            "iceberg_install_all_headers(iceberg/catalog)",
            read("src/iceberg/catalog/CMakeLists.txt"),
        )

    def test_core_tests_are_run_by_both_build_systems(self) -> None:
        self.assertIn(
            "snapshot_summary_builder_test.cc",
            read("src/iceberg/test/meson.build"),
        )

    def test_fallback_dependency_versions_match(self) -> None:
        versions = {
            "CRoaring": (
                extract(
                    r"CRoaring/.+?/v([0-9.]+)\.tar\.gz",
                    "cmake_modules/IcebergThirdpartyToolchain.cmake",
                ),
                extract(r"directory = CRoaring-([0-9.]+)", "subprojects/croaring.wrap"),
            ),
            "nlohmann/json": (
                extract(
                    r"nlohmann/json/releases/download/v([0-9.]+)",
                    "cmake_modules/IcebergThirdpartyToolchain.cmake",
                ),
                extract(
                    r"directory = nlohmann_json-([0-9.]+)",
                    "subprojects/nlohmann_json.wrap",
                ),
            ),
            "GoogleTest": (
                extract(
                    r"# release-([0-9.]+)",
                    "cmake_modules/IcebergThirdpartyToolchain.cmake",
                ),
                extract(r"directory = googletest-([0-9.]+)", "subprojects/gtest.wrap"),
            ),
        }
        mismatches = {
            name: pair for name, pair in versions.items() if pair[0] != pair[1]
        }
        self.assertEqual({}, mismatches)


if __name__ == "__main__":
    unittest.main()
