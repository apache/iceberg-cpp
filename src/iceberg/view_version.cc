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

#include "iceberg/view_version.h"

namespace iceberg {

const SqlViewRepresentation* GetSql(const ViewRepresentation& repr) {
  return std::get_if<SqlViewRepresentation>(&repr);
}

std::string_view RepresentationType(const ViewRepresentation& repr) {
  if (const auto* sql = GetSql(repr)) {
    return kSqlRepresentationType;
  }
  return std::get<UnknownViewRepresentation>(repr).type;
}

bool ViewVersion::Equals(const ViewVersion& other) const {
  return version_id == other.version_id && schema_id == other.schema_id &&
         timestamp_ms == other.timestamp_ms && summary == other.summary &&
         representations == other.representations &&
         default_catalog == other.default_catalog &&
         default_namespace == other.default_namespace;
}

}  // namespace iceberg
