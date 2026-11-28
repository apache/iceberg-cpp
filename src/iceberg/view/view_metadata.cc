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

#include "iceberg/view/view_metadata.h"

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/type_util.h"
#include "iceberg/util/uuid.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

Status ViewMetadata::Validate() const {
  if (format_version != kSupportedViewFormatVersion) {
    return Invalid("Unsupported view format version: {}",
                   static_cast<int>(format_version));
  }
  if (auto uuid = Uuid::FromString(view_uuid); !uuid.has_value()) {
    return Invalid("Invalid view UUID '{}': {}", view_uuid, uuid.error().message);
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
    std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>> fields;
    IdToFieldVisitor visitor(fields);
    if (auto status = VisitTypeInline(*schema, &visitor); !status.has_value()) {
      return Invalid("Invalid view schema {}: {}", schema->schema_id(),
                     status.error().message);
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
  for (const auto& entry : version_log) {
    if (!version_ids.contains(entry.version_id)) {
      return Invalid("View history references unlisted version ID {}", entry.version_id);
    }
  }

  for (const auto& version : versions) {
    if (!schema_ids.contains(version->schema_id)) {
      return Invalid("View version {} references unlisted schema ID {}",
                     version->version_id, version->schema_id);
    }
    std::unordered_set<std::string> dialects;
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
        if (!dialects.insert(StringUtils::ToLower(sql->dialect)).second) {
          return Invalid(
              "View version {} has multiple SQL representations for dialect '{}'",
              version->version_id, sql->dialect);
        }
      } else {
        const auto& unknown = std::get<UnknownViewRepresentation>(repr);
        if (unknown.type.empty()) {
          return Invalid("View version {} has an unknown representation without a type",
                         version->version_id);
        }
        // Java's equalsIgnoreCase also recognizes the long s (U+017F) as 's'.
        if (StringUtils::EqualsIgnoreCase(unknown.type, kSqlRepresentationType) ||
            StringUtils::EqualsIgnoreCase(unknown.type, "ſql")) {
          return Invalid("View version {} must use an SQL representation for type '{}'",
                         version->version_id, unknown.type);
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
