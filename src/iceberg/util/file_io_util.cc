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

#include "iceberg/util/file_io_util.h"

#include "iceberg/file_io_registry.h"

namespace iceberg {

std::string FileIOUtil::DetectFileIOName(std::string_view uri) {
  // Check for "scheme://" pattern
  auto pos = uri.find("://");
  if (pos != std::string_view::npos) {
    auto scheme = uri.substr(0, pos);
    if (scheme == "s3") {
      return std::string(FileIORegistry::kArrowS3FileIO);
    }
  }
  return std::string(FileIORegistry::kArrowLocalFileIO);
}

Result<std::unique_ptr<FileIO>> FileIOUtil::CreateFileIO(
    const std::unordered_map<std::string, std::string>& properties) {
  auto io_impl = properties.find(std::string(FileIOProperties::kImpl));
  if (io_impl == properties.end() || io_impl->second.empty()) {
    return InvalidArgument("\"{}\" property is required to create FileIO",
                           FileIOProperties::kImpl);
  }

  const auto& impl_name = io_impl->second;
  if (impl_name == FileIORegistry::kArrowS3FileIO ||
      impl_name == FileIORegistry::kArrowLocalFileIO) {
    auto warehouse_it = properties.find("warehouse");
    if (warehouse_it != properties.end() && !warehouse_it->second.empty()) {
      auto detected = DetectFileIOName(warehouse_it->second);
      if (detected != impl_name) {
        return InvalidArgument(
            "io-impl '{}' is incompatible with warehouse URI '{}' "
            "(detected scheme: '{}')",
            impl_name, warehouse_it->second, detected);
      }
    }
  }

  return FileIORegistry::Load(io_impl->second, properties);
}

}  // namespace iceberg
