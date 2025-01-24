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

mkdir ${build_dir}
pushd ${build_dir}

is_windows() {
    [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" ]]
}

CMAKE_ARGS=(
    "-DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX:-${ICEBERG_HOME}}"
)

if is_windows; then
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Release")
else
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Debug")
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}
if is_windows; then
  cmake --build . --config Release
else
  cmake --build .
fi

popd

# clean up between builds
rm -rf ${build_dir}
