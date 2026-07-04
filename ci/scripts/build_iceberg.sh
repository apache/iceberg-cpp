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
# Usage: build_iceberg.sh <source_dir> [rest_integration_tests=OFF] [sccache=OFF] [s3=OFF] [sigv4=OFF] [bundle_awssdk=ON] [build_type=Debug]

set -eux

source_dir=${1}
build_dir=${1}/build
build_rest_integration_test=${2:-OFF}
build_enable_sccache=${3:-OFF}
build_enable_s3=${4:-OFF}
build_enable_sigv4=${5:-OFF}
build_bundle_awssdk=${6:-ON}
run_tests=${ICEBERG_RUN_TESTS:-ON}

mkdir ${build_dir}
pushd ${build_dir}

is_windows() {
    [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" || "${OSTYPE}" == "cygwin" ]]
}

if is_windows; then
    build_type=${7:-Release}
else
    build_type=${7:-Debug}
fi

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
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=${build_type}")
else
    # Pass an externally provided toolchain (e.g. vcpkg for the SigV4 job)
    if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
        CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
    fi
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=${build_type}")
fi

if [[ "${build_enable_sccache}" == "ON" ]]; then
    CMAKE_ARGS+=("-DCMAKE_CXX_COMPILER_LAUNCHER=sccache")
    CMAKE_ARGS+=("-DCMAKE_C_COMPILER_LAUNCHER=sccache")
fi

if [[ -n "${ICEBERG_EXTRA_CMAKE_ARGS:-}" ]]; then
    read -r -a EXTRA_CMAKE_ARGS <<< "${ICEBERG_EXTRA_CMAKE_ARGS}"
    CMAKE_ARGS+=("${EXTRA_CMAKE_ARGS[@]}")
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}
if is_windows; then
  cmake --build . --config "${build_type}" --target install
  if [[ "${run_tests}" == "ON" ]]; then
    ctest --output-on-failure -C "${build_type}"
  fi
else
  cmake --build . --target install
  if [[ "${run_tests}" == "ON" ]]; then
    ctest --output-on-failure
  fi
fi

popd

# clean up between builds
rm -rf ${build_dir}
