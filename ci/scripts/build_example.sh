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

usage() {
    cat <<'EOF'
Usage: build_example.sh --source-dir DIR [options]

Options:
  --build-type TYPE      CMake build type (default: Debug)
  --cxx-standard N       C++ standard for the example (default: 23)
  --run-example ON|OFF   Run the example after building (default: OFF)
  -h, --help             Show this help
EOF
}

require_on_off() {
    case "${2}" in
        ON|OFF) ;;
        *)
            echo "$1 must be ON or OFF, got '${2:-}'" >&2
            exit 2
            ;;
    esac
}

set -eu

source_dir=
build_type=Debug
cxx_standard=23
run_example=OFF

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source-dir)
            source_dir=${2:-}
            shift 2
            ;;
        --build-type)
            build_type=${2:-}
            shift 2
            ;;
        --cxx-standard)
            cxx_standard=${2:-}
            shift 2
            ;;
        --run-example)
            require_on_off "$1" "${2:-}"
            run_example=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${source_dir}" || -z "${build_type}" || -z "${cxx_standard}" ]]; then
    usage >&2
    exit 2
fi

set -x
build_dir=${source_dir}/build

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

CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=${build_type}")
CMAKE_ARGS+=("-DICEBERG_EXAMPLE_CXX_STANDARD=${cxx_standard}")

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
