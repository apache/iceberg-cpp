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

namespace iceberg {

Status ManifestWriterV1::WriteManifestEntry(const ManifestEntry& entry) const {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV1::Close() { return {}; }

Status ManifestWriterV2::WriteManifestEntry(const ManifestEntry& entry) const {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV2::Close() { return {}; }

Status ManifestWriterV3::WriteManifestEntry(const ManifestEntry& entry) const {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV3::Close() { return {}; }

Status ManifestListWriterV1::WriteManifestFile(const ManifestFile& file) const {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV1::Close() { return {}; }

Status ManifestListWriterV2::WriteManifestFile(const ManifestFile& file) const {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV2::Close() { return {}; }

Status ManifestListWriterV3::WriteManifestFile(const ManifestFile& file) const {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV3::Close() { return {}; }
}  // namespace iceberg
