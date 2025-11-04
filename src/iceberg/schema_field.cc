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

#include "iceberg/schema_field.h"

#include <format>

#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

SchemaField::SchemaField(int32_t field_id, std::string name, std::shared_ptr<Type> type,
                         bool optional, std::string doc)
    : field_id_(field_id),
      name_(std::move(name)),
      type_(std::move(type)),
      optional_(optional),
      doc_(std::move(doc)) {}

SchemaField SchemaField::MakeOptional(int32_t field_id, std::string name,
                                      std::shared_ptr<Type> type, std::string doc) {
  return {field_id, std::move(name), std::move(type), true, std::move(doc)};
}

SchemaField SchemaField::MakeRequired(int32_t field_id, std::string name,
                                      std::shared_ptr<Type> type, std::string doc) {
  return {field_id, std::move(name), std::move(type), false, std::move(doc)};
}

int32_t SchemaField::field_id() const { return field_id_; }

std::string_view SchemaField::name() const { return name_; }

const std::shared_ptr<Type>& SchemaField::type() const { return type_; }

bool SchemaField::optional() const { return optional_; }

std::string_view SchemaField::doc() const { return doc_; }

Status SchemaField::Validate() const {
  if (name_.empty()) [[unlikely]] {
    return InvalidSchema("SchemaField cannot have empty name");
  }
  if (type_ == nullptr) [[unlikely]] {
    return InvalidSchema("SchemaField cannot have null type");
  }
  return {};
}

std::string SchemaField::ToString() const {
  std::string result = std::format("{} ({}): {} ({}){}", name_, field_id_, *type_,
                                   optional_ ? "optional" : "required",
                                   !doc_.empty() ? std::format(" - {}", doc_) : "");
  return result;
}

bool SchemaField::Equals(const SchemaField& other) const {
  return field_id_ == other.field_id_ && name_ == other.name_ && *type_ == *other.type_ &&
         optional_ == other.optional_;
}

}  // namespace iceberg
