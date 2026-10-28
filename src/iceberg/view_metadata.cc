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

#include "iceberg/view_metadata.h"

#include <algorithm>
#include <unordered_set>

#include "iceberg/schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

Status ViewMetadata::Validate() const {
  if (format_version != kSupportedViewFormatVersion) {
    return Invalid("Unsupported view format version: {}",
                   static_cast<int>(format_version));
  }
  if (view_uuid.empty()) {
    return Invalid("View UUID must not be empty");
  }
  if (location.empty()) {
    return Invalid("View location must not be empty");
  }
  if (schemas.empty()) {
    return Invalid("View metadata must list at least one schema");
  }

  std::unordered_set<int32_t> schema_ids;
  for (const auto& schema : schemas) {
    if (schema == nullptr) {
      return Invalid("View schemas must not be null");
    }
    if (!schema_ids.insert(schema->schema_id()).second) {
      return Invalid("Duplicate view schema ID: {}", schema->schema_id());
    }
  }

  if (versions.empty()) {
    return Invalid("View metadata must list at least one version");
  }
  std::unordered_set<int32_t> version_ids;
  for (const auto& version : versions) {
    if (version == nullptr) {
      return Invalid("View versions must not be null");
    }
    if (!version_ids.insert(version->version_id).second) {
      return Invalid("Duplicate view version ID: {}", version->version_id);
    }
  }
  if (!version_ids.contains(current_version_id)) {
    return Invalid("Current version ID {} is not a listed view version",
                   current_version_id);
  }

  for (const auto& version : versions) {
    if (!schema_ids.contains(version->schema_id)) {
      return Invalid("View version {} references unlisted schema ID {}",
                     version->version_id, version->schema_id);
    }
    if (version->default_namespace.levels.empty()) {
      return Invalid("View version {} must have a non-empty default namespace",
                     version->version_id);
    }

    std::vector<std::string_view> dialects;
    for (const auto& repr : version->representations) {
      if (const auto* sql = GetSql(repr)) {
        if (sql->sql.empty()) {
          return Invalid("View version {} has an SQL representation without SQL",
                         version->version_id);
        }
        if (sql->dialect.empty()) {
          return Invalid("View version {} has an SQL representation without a dialect",
                         version->version_id);
        }
        for (const auto& dialect : dialects) {
          if (StringUtils::EqualsIgnoreCaseAscii(dialect, sql->dialect)) {
            return Invalid(
                "View version {} has multiple SQL representations for dialect '{}'",
                version->version_id, sql->dialect);
          }
        }
        dialects.push_back(sql->dialect);
      } else {
        const auto& unknown = std::get<UnknownViewRepresentation>(repr);
        if (unknown.type.empty()) {
          return Invalid("View version {} has an unknown representation without a type",
                         version->version_id);
        }
      }
    }
  }
  return {};
}

Result<std::shared_ptr<ViewVersion>> ViewMetadata::CurrentVersion() const {
  return VersionById(current_version_id);
}

Result<std::shared_ptr<ViewVersion>> ViewMetadata::VersionById(int32_t version_id) const {
  auto iter = std::ranges::find_if(versions, [version_id](const auto& version) {
    return version != nullptr && version->version_id == version_id;
  });
  if (iter == versions.end()) {
    return NotFound("View version with ID {} is not found", version_id);
  }
  return *iter;
}

Result<std::shared_ptr<Schema>> ViewMetadata::CurrentSchema() const {
  ICEBERG_ASSIGN_OR_RAISE(auto version, CurrentVersion());
  return SchemaById(version->schema_id);
}

Result<std::shared_ptr<Schema>> ViewMetadata::SchemaById(int32_t schema_id) const {
  auto iter = std::ranges::find_if(schemas, [schema_id](const auto& schema) {
    return schema != nullptr && schema->schema_id() == schema_id;
  });
  if (iter == schemas.end()) {
    return NotFound("View schema with ID {} is not found", schema_id);
  }
  return *iter;
}

bool ViewMetadata::Equals(const ViewMetadata& other) const {
  auto shared_ptr_equals = [](const auto& lhs, const auto& rhs) {
    if (lhs == nullptr || rhs == nullptr) {
      return lhs == rhs;
    }
    return *lhs == *rhs;
  };
  return format_version == other.format_version && view_uuid == other.view_uuid &&
         location == other.location && schemas.size() == other.schemas.size() &&
         std::equal(schemas.begin(), schemas.end(), other.schemas.begin(),
                    shared_ptr_equals) &&
         current_version_id == other.current_version_id &&
         versions.size() == other.versions.size() &&
         std::equal(versions.begin(), versions.end(), other.versions.begin(),
                    shared_ptr_equals) &&
         version_log == other.version_log && properties == other.properties;
}

}  // namespace iceberg
