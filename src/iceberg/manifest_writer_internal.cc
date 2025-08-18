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

Status ManifestWriterV1::Add(const ManifestEntry& entry) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV1::AddAll(const std::vector<ManifestEntry>& files) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV1::Close() { return {}; }

ManifestEntry ManifestWriterV1::prepare(const ManifestEntry& entry) {
  return wrapper_.Wrap(entry);
}

Status ManifestWriterV2::Add(const ManifestEntry& entry) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV2::AddAll(const std::vector<ManifestEntry>& files) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV2::Close() { return {}; }

ManifestEntry ManifestWriterV2::prepare(const ManifestEntry& entry) {
  return wrapper_.Wrap(entry);
}

Status ManifestWriterV3::Add(const ManifestEntry& entry) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV3::AddAll(const std::vector<ManifestEntry>& files) {
  // TODO(xiao.dong) convert entries to arrow data
  return {};
}

Status ManifestWriterV3::Close() { return {}; }

ManifestEntry ManifestWriterV3::prepare(const ManifestEntry& entry) {
  return wrapper_.Wrap(entry);
}

Status ManifestListWriterV1::Add(const ManifestFile& file) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV1::AddAll(const std::vector<ManifestFile>& files) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV1::Close() { return {}; }

ManifestFile ManifestListWriterV1::prepare(const ManifestFile& file) {
  return wrapper_.Wrap(file);
}

Status ManifestListWriterV2::Add(const ManifestFile& file) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV2::AddAll(const std::vector<ManifestFile>& files) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV2::Close() { return {}; }

ManifestFile ManifestListWriterV2::prepare(const ManifestFile& file) {
  return wrapper_.Wrap(file);
}

Status ManifestListWriterV3::Add(const ManifestFile& file) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV3::AddAll(const std::vector<ManifestFile>& files) {
  // TODO(xiao.dong) convert manifest files to arrow data
  return {};
}

Status ManifestListWriterV3::Close() { return {}; }

ManifestFile ManifestListWriterV3::prepare(const ManifestFile& file) {
  if (file.content != ManifestFile::Content::kData || file.first_row_id.has_value()) {
    return wrapper_.Wrap(file, std::nullopt);
  }
  auto result = wrapper_.Wrap(file, next_row_id_);
  next_row_id_ +=
      file.existing_rows_count.value_or(0) + file.added_rows_count.value_or(0);
  return result;
}
}  // namespace iceberg
