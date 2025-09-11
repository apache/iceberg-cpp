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
  echo " e.g.: $0 0.1.0 0"
  exit 1
fi

set -o pipefail
set -x

VERSION="$1"
RC="$2"

ICEBERG_DIST_BASE_URL="https://downloads.apache.org/iceberg"
DOWNLOAD_RC_BASE_URL="https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${VERSION}-rc${RC}"
ARCHIVE_BASE_NAME="apache-iceberg-cpp-${VERSION}"

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

test_source_distribution() {
  echo "Building and testing Apache Iceberg C++..."

  # Configure build
  cmake -S . -B build \
    -DCMAKE_CXX_FLAGS="-Wno-deprecated-declarations" \
    -DCMAKE_BUILD_TYPE=Release \
    -DICEBERG_BUILD_STATIC=ON \
    -DICEBERG_BUILD_SHARED=ON \
    --compile-no-warning-as-error

  # Build
  cmake --build build --parallel $(nproc || sysctl -n hw.ncpu || echo 4)

  # Run tests
  ctest --test-dir build --output-on-failure --parallel $(nproc || sysctl -n hw.ncpu || echo 4)

  # Install
  mkdir -p ./install_test
  cmake --install build --prefix ./install_test

  echo "Build, test and install completed successfully!"
}

setup_tmpdir "iceberg-cpp-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"

import_gpg_keys
fetch_archive
ensure_source_directory
pushd "${ARCHIVE_BASE_NAME}"
test_source_distribution
popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
