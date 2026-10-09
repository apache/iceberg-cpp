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
usage() {
    cat <<'EOF'
Usage: build_iceberg.sh --source-dir DIR [options]

Options:
  --build-type TYPE                 CMake build type (default: Debug)
  --run-tests ON|OFF                Run CTest after building (default: ON)
  --sccache ON|OFF                  Use sccache (default: OFF)
  --rest-integration-tests ON|OFF   Build REST integration tests (default: OFF)
  --s3 ON|OFF                       Enable S3 support (default: OFF)
  --sigv4 ON|OFF                    Enable SigV4 support (default: OFF)
  --bundle-awssdk ON|OFF            Bundle the AWS SDK (default: ON)
  -h, --help                        Show this help
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
build_enable_sccache=OFF
build_rest_integration_test=OFF
build_enable_s3=OFF
build_enable_sigv4=OFF
build_bundle_awssdk=ON
run_tests=ON

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
        --run-tests)
            require_on_off "$1" "${2:-}"
            run_tests=$2
            shift 2
            ;;
        --sccache)
            require_on_off "$1" "${2:-}"
            build_enable_sccache=$2
            shift 2
            ;;
        --rest-integration-tests)
            require_on_off "$1" "${2:-}"
            build_rest_integration_test=$2
            shift 2
            ;;
        --s3)
            require_on_off "$1" "${2:-}"
            build_enable_s3=$2
            shift 2
            ;;
        --sigv4)
            require_on_off "$1" "${2:-}"
            build_enable_sigv4=$2
            shift 2
            ;;
        --bundle-awssdk)
            require_on_off "$1" "${2:-}"
            build_bundle_awssdk=$2
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

if [[ -z "${source_dir}" || -z "${build_type}" ]]; then
    usage >&2
    exit 2
fi

set -x
build_dir=${source_dir}/build

mkdir ${build_dir}
pushd ${build_dir}

is_windows() {
    [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" || "${OSTYPE}" == "cygwin" ]]
}

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX:-${ICEBERG_HOME}}"
    "-DICEBERG_BUILD_STATIC=ON"
    "-DICEBERG_BUILD_SHARED=ON"
    "-DICEBERG_BUILD_REST_INTEGRATION_TESTS=${build_rest_integration_test}"
)

if [[ "${build_enable_s3}" == "ON" ]]; then
    CMAKE_ARGS+=("-DICEBERG_S3=ON")
else
    CMAKE_ARGS+=("-DICEBERG_S3=OFF")
fi

if [[ "${build_enable_sigv4}" == "ON" ]]; then
    CMAKE_ARGS+=("-DICEBERG_SIGV4=ON")
else
    CMAKE_ARGS+=("-DICEBERG_SIGV4=OFF")
fi

if [[ "${build_bundle_awssdk}" == "ON" ]]; then
    CMAKE_ARGS+=("-DICEBERG_BUNDLE_AWSSDK=ON")
else
    CMAKE_ARGS+=("-DICEBERG_BUNDLE_AWSSDK=OFF")
fi

if is_windows; then
    CMAKE_TOOLCHAIN_FILE="${CMAKE_TOOLCHAIN_FILE:-C:/vcpkg/scripts/buildsystems/vcpkg.cmake}"
fi

# Pass an externally provided toolchain, or the default Windows vcpkg toolchain.
if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=${build_type}")

if [[ "${build_enable_sccache}" == "ON" ]]; then
    CMAKE_ARGS+=("-DCMAKE_CXX_COMPILER_LAUNCHER=sccache")
    CMAKE_ARGS+=("-DCMAKE_C_COMPILER_LAUNCHER=sccache")
fi

if [[ -n "${ICEBERG_EXTRA_CMAKE_ARGS:-}" ]]; then
    read -r -a EXTRA_CMAKE_ARGS <<< "${ICEBERG_EXTRA_CMAKE_ARGS}"
    CMAKE_ARGS+=("${EXTRA_CMAKE_ARGS[@]}")
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}

cmake --build . --target install
if [[ "${run_tests}" == "ON" ]]; then
    ctest --output-on-failure
fi

popd

# Clean up after the build. Windows can briefly hold a just-built exe/dll,
# so retry but do not fail an otherwise successful CI job.
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
