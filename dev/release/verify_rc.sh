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

set -eu

for cmd in curl gpg cmake; do
  if ! command -v ${cmd} &> /dev/null; then
    echo "This script requires '${cmd}' but it's not installed. Aborting."
    exit 1
  fi
done

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="$(dirname "$(dirname "${SOURCE_DIR}")")"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 0.1.0 1"
  exit 1
fi

set -o pipefail
set -x

VERSION="$1"
RC="$2"

ICEBERG_DIST_BASE_URL="https://downloads.apache.org/iceberg"
DOWNLOAD_RC_BASE_URL="https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${VERSION}-rc${RC}"
ARCHIVE_BASE_NAME="apache-iceberg-cpp-${VERSION}-rc${RC}"

: "${VERIFY_DEFAULT:=1}"
: "${VERIFY_DOWNLOAD:=${VERIFY_DEFAULT}}"
: "${VERIFY_SIGN:=${VERIFY_DEFAULT}}"

VERIFY_SUCCESS=no

setup_tmpdir() {
  cleanup() {
    if [ "${VERIFY_SUCCESS}" = "yes" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details."
    fi
  }

  if [ -z "${VERIFY_TMPDIR:-}" ]; then
    VERIFY_TMPDIR="$(mktemp -d -t "$1.XXXXX")"
    trap cleanup EXIT
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

download() {
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "$1"
}

download_rc_file() {
  if [ "${VERIFY_DOWNLOAD}" -gt 0 ]; then
    download "${DOWNLOAD_RC_BASE_URL}/$1"
  else
    cp "${TOP_SOURCE_DIR}/$1" "$1"
  fi
}

import_gpg_keys() {
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download "${ICEBERG_DIST_BASE_URL}/KEYS"
    gpg --import KEYS
  fi
}

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 -c"
else
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz"
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.asc"
    gpg --verify "${ARCHIVE_BASE_NAME}.tar.gz.asc" "${ARCHIVE_BASE_NAME}.tar.gz"
  fi
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
  ${sha512_verify} "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
}

ensure_source_directory() {
  tar xf "${ARCHIVE_BASE_NAME}".tar.gz
}

check_compiler() {
  echo "--- Verifying Build Environment ---"

  # Check for minimum CMake version (e.g., 3.25)
  MIN_CMAKE_VERSION="3.25"
  CMAKE_VERSION=$(cmake --version | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
  echo "Found CMake version: $CMAKE_VERSION"
  if [ "$(printf '%s\n' "${MIN_CMAKE_VERSION}" "${CMAKE_VERSION}" | sort -V | head -n1)" != "${MIN_CMAKE_VERSION}" ]; then
    echo "ERROR: CMake ${MIN_CMAKE_VERSION} or higher is required, but found ${CMAKE_VERSION}."
    exit 1
  fi

  # Check for C++23 compliant compiler
  local compiler_ok=0
  if command -v g++ >/dev/null 2>&1; then
    # Get major version: g++ (Debian 13.2.0-4) 13.2.0 -> 13
    GCC_MAJOR_VERSION=$(g++ -dumpversion | cut -d. -f1)
    echo "Found GCC version: $(g++ --version | head -n1)"
    # GCC 13 is the first version with full C++23 support
    if [ "${GCC_MAJOR_VERSION}" -ge 13 ]; then
      echo "GCC version is sufficient for C++23."
      compiler_ok=1
    fi
  fi

  if [ "${compiler_ok}" -eq 0 ] && command -v clang++ >/dev/null 2>&1; then
    CLANG_MAJOR_VERSION=$(clang++ --version | head -n1 | grep -oE '[0-9]+' | head -n1)
    echo "Found Clang version: $(clang++ --version | head -n1)"
    # Clang 16 is the first version with full C++23 support
    if [ "${CLANG_MAJOR_VERSION}" -ge 16 ]; then
      echo "Clang version is sufficient for C++23."
      compiler_ok=1
    fi
  fi

  if [ "${compiler_ok}" -eq 0 ]; then
    echo "ERROR: No C++23 compliant compiler found (GCC 13+ or Clang 16+ required)."
    exit 1
  fi

  echo "--- Environment check passed ---"
}

test_source_distribution() {
  echo "Building and testing Apache Iceberg C++..."

  # Configure build
  cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DICEBERG_BUILD_STATIC=ON \
    -DICEBERG_BUILD_SHARED=ON

  # Build
  cmake --build build --parallel $(nproc || sysctl -n hw.ncpu || echo 4)

  # Run tests
  ctest --test-dir build --output-on-failure --parallel $(nproc || sysctl -n hw.ncpu || echo 4)

  echo "Build and test completed successfully!"
}

setup_tmpdir "iceberg-cpp-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"

import_gpg_keys
fetch_archive
ensure_source_directory
check_compiler
pushd "${ARCHIVE_BASE_NAME}"
test_source_distribution
popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
