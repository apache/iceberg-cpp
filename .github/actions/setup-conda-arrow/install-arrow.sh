#!/usr/bin/env bash
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
#
# Installs Arrow and Parquet into the active conda environment and exports the
# paths CMake and the test binaries need. Run from the setup-conda-arrow action.

set -euo pipefail

toolchain="${GITHUB_WORKSPACE}/cmake_modules/IcebergThirdpartyToolchain.cmake"

# Keep conda Arrow in sync with the vendored build.
arrow_version=$(sed -nE 's/^set\(ICEBERG_ARROW_BUILD_VERSION "([0-9]+\.[0-9]+\.[0-9]+)"\)$/\1/p' "${toolchain}")
if [[ -z "${arrow_version}" ]]; then
    echo "::error::Could not read ICEBERG_ARROW_BUILD_VERSION from ${toolchain}"
    exit 1
fi

packages=("libarrow==${arrow_version}" "libparquet==${arrow_version}")
if [[ -n "${EXTRA_PACKAGES:-}" ]]; then
    read -r -a extra_packages <<< "${EXTRA_PACKAGES}"
    packages+=("${extra_packages[@]}")
fi
mamba install -y "${packages[@]}"

echo "CMAKE_PREFIX_PATH=${CONDA_PREFIX}" >> "${GITHUB_ENV}"
# build_iceberg.sh reads this and fails if CMake configured vendored Arrow anyway.
echo "ICEBERG_REQUIRE_SYSTEM_ARROW=ON" >> "${GITHUB_ENV}"

if [[ "${RUNNER_OS}" == "macOS" ]]; then
    echo "DYLD_FALLBACK_LIBRARY_PATH=${DYLD_FALLBACK_LIBRARY_PATH:+${DYLD_FALLBACK_LIBRARY_PATH}:}${CONDA_PREFIX}/lib" >> "${GITHUB_ENV}"
else
    echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH:+${LD_LIBRARY_PATH}:}${CONDA_PREFIX}/lib" >> "${GITHUB_ENV}"
fi
