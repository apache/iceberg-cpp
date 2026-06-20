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
# Regenerate the committed Hive Metastore C++ bindings from the vendored
# Thrift IDL. Run this whenever thirdparty/hive_metastore/*.thrift changes.
# Requires the Apache Thrift IDL compiler ("thrift") on PATH; the generated
# sources are checked in so a normal build needs no Thrift compiler.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
idl_dir="${repo_root}/thirdparty/hive_metastore"
out_dir="${repo_root}/src/iceberg/catalog/hive/gen-cpp"

if ! command -v thrift >/dev/null 2>&1; then
  echo "error: 'thrift' IDL compiler not found on PATH" >&2
  echo "       install it (e.g. 'brew install thrift' or 'apt install thrift-compiler')" >&2
  exit 1
fi

rm -rf "${out_dir}"
mkdir -p "${out_dir}"

# -r recurses into included IDL (fb303); cpp:no_skeleton omits server stubs.
thrift -r --gen cpp:no_skeleton -out "${out_dir}" \
  -I "${idl_dir}" "${idl_dir}/hive_metastore.thrift"

echo "Regenerated Hive Metastore bindings in ${out_dir}"
echo "Review the diff and commit the result."
