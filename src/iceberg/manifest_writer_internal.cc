/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "manifest_writer_internal.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"

namespace iceberg {

Status ManifestWriterImpl::WriteManifestEntries(
    const std::vector<ManifestEntry>& /*entries*/) const {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestListWriterImpl::WriteManifestFiles(
    const std::vector<ManifestFile>& /*files*/) const {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

}  // namespace iceberg
