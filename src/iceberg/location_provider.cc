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

#include "iceberg/location_provider.h"

#include "iceberg/partition_spec.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/murmurhash3_internal.h"

namespace iceberg {

namespace {

constexpr uint8_t kEntropyDirMask = 0x0f;
constexpr uint8_t kRestDirMask = 0xff;
constexpr int32_t kHashBits = 20;
constexpr int32_t kEntropyDirLength = 4;
constexpr int32_t kEntropyDirDepth = 3;

std::string DataLocation(const TableProperties& properties,
                         std::string_view table_location) {
  auto data_location = properties.Get(TableProperties::kWriteDataLocation);
  if (data_location.empty()) {
    data_location = std::format("{}/data", table_location);
  }
  return data_location;
}

std::string PathContext(std::string_view table_location) {
  std::string_view path = LocationUtil::StripTrailingSlash(table_location);

  size_t last_slash = path.find_last_of('/');
  if (last_slash != std::string::npos && last_slash < path.length() - 1) {
    std::string_view data_path = path.substr(last_slash + 1);
    std::string_view parent_path(path.data(), last_slash);
    size_t parent_last_slash = parent_path.find_last_of('/');

    if (parent_last_slash != std::string::npos) {
      std::string_view parent_name = parent_path.substr(parent_last_slash + 1);
      return std::format("{}/{}", parent_name, data_path);
    } else {
      return std::format("{}/{}", parent_path, data_path);
    }
  }

  return std::string(table_location);
}

/// \brief Divides hash into directories for optimized orphan removal operation using
/// kEntropyDirDepth and kEntropyDirLength.
///
/// If the low `kHashBits = 20` of `hash` is '10011001100110011001', then return
/// '1001/1001/1001/10011001' with depth 3 and length 4.
///
/// \param hash The hash value to be divided.
/// \return The path according to the `hash` value.
std::string DirsFromHash(int32_t hash) {
  std::string hash_with_dirs;

  for (int32_t i = 0; i < kEntropyDirDepth * kEntropyDirLength; i += kEntropyDirLength) {
    if (i > 0) {
      hash_with_dirs += "/";
    }
    uint8_t dir_bits = kEntropyDirMask & (hash >> (kHashBits - i - kEntropyDirLength));
    hash_with_dirs += std::format("{:04b}", dir_bits);
  }

  hash_with_dirs += "/";
  uint8_t rest_bits = kRestDirMask & hash;
  hash_with_dirs += std::format("{:08b}", rest_bits);

  return hash_with_dirs;
}

std::string ComputeHash(std::string_view file_name) {
  int32_t hash_value = 0;
  MurmurHash3_x86_32(file_name.data(), file_name.size(), 0, &hash_value);
  return DirsFromHash(hash_value);
}

}  // namespace

/// \brief DefaultLocationProvider privides default location provider for local file
/// system.
class DefaultLocationProvider : public LocationProvider {
 public:
  DefaultLocationProvider(std::string_view table_location,
                          const TableProperties& properties);

  std::string NewDataLocation(std::string_view filename) override;

  Result<std::string> NewDataLocation(const PartitionSpec& spec,
                                      const PartitionValues& partition_data,
                                      std::string_view filename) override;

 private:
  std::string data_location_;
};

// Implementation of DefaultLocationProvider
DefaultLocationProvider::DefaultLocationProvider(std::string_view table_location,
                                                 const TableProperties& properties)
    : data_location_(
          LocationUtil::StripTrailingSlash(DataLocation(properties, table_location))) {}

std::string DefaultLocationProvider::NewDataLocation(std::string_view filename) {
  return std::format("{}/{}", data_location_, filename);
}

Result<std::string> DefaultLocationProvider::NewDataLocation(
    const PartitionSpec& spec, const PartitionValues& partition_data,
    std::string_view filename) {
  ICEBERG_ASSIGN_OR_RAISE(auto partition_path, spec.PartitionPath(partition_data));
  return std::format("{}/{}/{}", data_location_, partition_path, filename);
}

/// \brief ObjectStoreLocationProvider provides location provider for object stores.
class ObjectStoreLocationProvider : public LocationProvider {
 public:
  ObjectStoreLocationProvider(std::string_view table_location,
                              const TableProperties& properties);

  std::string NewDataLocation(std::string_view filename) override;

  Result<std::string> NewDataLocation(const PartitionSpec& spec,
                                      const PartitionValues& partition_data,
                                      std::string_view filename) override;

 private:
  std::string storage_location_;
  std::string context_;
  bool include_partition_paths_;
};

// Implementation of ObjectStoreLocationProvider
ObjectStoreLocationProvider::ObjectStoreLocationProvider(
    std::string_view table_location, const TableProperties& properties)
    : include_partition_paths_(
          properties.Get(TableProperties::kWriteObjectStorePartitionedPaths)) {
  storage_location_ =
      LocationUtil::StripTrailingSlash(DataLocation(properties, table_location));

  // If the storage location is within the table prefix, don't add table and database name
  // context
  if (!storage_location_.starts_with(table_location)) {
    context_ = PathContext(table_location);
  }
}

std::string ObjectStoreLocationProvider::NewDataLocation(std::string_view filename) {
  std::string hash = ComputeHash(filename);

  if (!context_.empty()) {
    return std::format("{}/{}/{}/{}", storage_location_, hash, context_, filename);
  } else {
    // If partition paths are included, add last part of entropy as dir before partition
    // names
    if (include_partition_paths_) {
      return std::format("{}/{}/{}", storage_location_, hash, filename);
    } else {
      // If partition paths are not included, append last part of entropy with `-` to file
      // name
      return std::format("{}/{}-{}", storage_location_, hash, filename);
    }
  }
}

Result<std::string> ObjectStoreLocationProvider::NewDataLocation(
    const PartitionSpec& spec, const PartitionValues& partition_data,
    std::string_view filename) {
  if (include_partition_paths_) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_path, spec.PartitionPath(partition_data));
    return NewDataLocation(std::format("{}/{}", partition_path, filename));
  } else {
    return NewDataLocation(filename);
  }
}

Result<std::unique_ptr<LocationProvider>> LocationProvider::Make(
    const std::string& input_location, const TableProperties& properties) {
  std::string_view location = LocationUtil::StripTrailingSlash(input_location);

  // TODO(xxx): Support dynamic constructor according to kWriteLocationProviderImpl

  if (properties.Get(TableProperties::kObjectStoreEnabled)) {
    return std::make_unique<ObjectStoreLocationProvider>(location, properties);
  } else {
    return std::make_unique<DefaultLocationProvider>(location, properties);
  }
}

}  // namespace iceberg
