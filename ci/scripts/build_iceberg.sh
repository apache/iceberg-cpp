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
build_rest_integration_test=${2:-OFF}
build_enable_sccache=${3:-OFF}

mkdir ${build_dir}
pushd ${build_dir}

is_windows() {
    [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" ]]
}

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX:-${ICEBERG_HOME}}"
    "-DICEBERG_BUILD_STATIC=ON"
    "-DICEBERG_BUILD_SHARED=ON"
    "-DICEBERG_BUILD_REST_INTEGRATION_TESTS=${build_rest_integration_test}"
)

if is_windows; then
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Release")
else
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Debug")
fi

if [[ "${build_enable_sccache}" == "ON" ]]; then
    CMAKE_ARGS+=("-DCMAKE_CXX_COMPILER_LAUNCHER=sccache")
    CMAKE_ARGS+=("-DCMAKE_C_COMPILER_LAUNCHER=sccache")
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}
if is_windows; then
  cmake --build . --config Release --target install
  ctest --output-on-failure -C Release
else
  cmake --build . --target install
  ctest --output-on-failure
fi

popd

# clean up between builds
rm -rf ${build_dir}
