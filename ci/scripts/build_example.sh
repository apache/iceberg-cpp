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

set -eux

source_dir=${1}
build_dir=${1}/build
run_example=${ICEBERG_RUN_EXAMPLE:-OFF}

# Clean up before configuring. If Windows still holds a just-built exe/dll
# after the retries, let mkdir fail rather than reuse a half-deleted tree.
for attempt in 1 2 3; do
    if rm -rf "${build_dir}"; then
        break
    fi
    if [[ "${attempt}" != "3" ]]; then
        sleep 2
    else
        echo "Failed to remove build directory after 3 attempts: ${build_dir}" >&2
    fi
done
mkdir "${build_dir}"
pushd ${build_dir}

is_windows() {
    [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" || "${OSTYPE}" == "cygwin" ]]
}

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX:-${ICEBERG_HOME}}"
)

if is_windows; then
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
fi

build_type="${ICEBERG_BUILD_TYPE:-Debug}"
CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=${build_type}")

cmake "${CMAKE_ARGS[@]}" ${source_dir}
cmake --build .
if [[ "${run_example}" == "ON" ]]; then
    if is_windows; then
        ./demo_example.exe
    else
        ./demo_example
    fi
fi

popd
