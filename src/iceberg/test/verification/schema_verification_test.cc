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

// Runs the `table-spec/schema` surface: project(parse(input)) == decoded, where the
// projection is one row per field in document order, read through public accessors.

#include <algorithm>
#include <array>
#include <exception>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_serde_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/verification/verification_util.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::verification {

namespace {

constexpr std::array<std::string_view, 2> kSubdirs = {"core", "unknown"};

// Fields of the `decoded` projection specific to this surface.
constexpr std::string_view kSchemaId = "schema-id";
constexpr std::string_view kIdentifierFieldIds = "identifier-field-ids";
constexpr std::string_view kParent = "parent";
constexpr std::string_view kDoc = "doc";

/// \brief The `type` of a row: an object for decimal and fixed, else the type name.
nlohmann::json ProjectFieldType(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kDecimal: {
      const auto& decimal = internal::checked_cast<const DecimalType&>(type);
      return {{kType, "decimal"},
              {kPrecision, decimal.precision()},
              {kScale, decimal.scale()}};
    }
    case TypeId::kFixed:
      return {{kType, "fixed"},
              {kLength, internal::checked_cast<const FixedType&>(type).length()}};
    default:
      return std::string(ToString(type.type_id()));
  }
}

/// \brief Append a row for each field of `type`, each followed by its children.
void AppendRows(const NestedType& type, std::optional<int32_t> parent_id,
                nlohmann::json* rows) {
  for (const auto& field : type.fields()) {
    rows->push_back({
        {kId, field.field_id()},
        {kName, std::string(field.name())},
        {kParent, parent_id.has_value() ? nlohmann::json(*parent_id) : nullptr},
        {kRequired, !field.optional()},
        {kType, ProjectFieldType(*field.type())},
        {kDoc, field.doc().empty() ? nullptr : nlohmann::json(std::string(field.doc()))},
    });
    if (field.type()->is_nested()) {
      AppendRows(internal::checked_cast<const NestedType&>(*field.type()),
                 field.field_id(), rows);
    }
  }
}

nlohmann::json ProjectSchema(const Schema& schema) {
  nlohmann::json rows = nlohmann::json::array();
  AppendRows(schema, std::nullopt, &rows);
  return {{kSchemaId, schema.schema_id()},
          {kIdentifierFieldIds, schema.IdentifierFieldIds()},
          {kFields, std::move(rows)}};
}

std::vector<std::string> CheckCase(const nlohmann::json& c) {
  std::optional<Result<std::unique_ptr<Schema>>> parsed;
  try {
    parsed = SchemaFromJson(c.at(kInput));
  } catch (const std::exception& e) {
    return {std::format("parse threw instead of returning an error: {}", e.what())};
  }

  if (!c.at(kValid).get<bool>()) {
    if (parsed->has_value()) {
      return {std::format("expected rejection, but parsed as {}",
                          parsed->value()->ToString())};
    }
    return {};
  }
  if (!parsed->has_value()) {
    return {std::format("parse failed: {}", parsed->error().message)};
  }

  // Rows keep document order, which is the column order under test, but the spec
  // calls identifier-field-ids a set.
  auto sorted_ids = [](const nlohmann::json& ids) {
    auto values = ids.get<std::vector<int32_t>>();
    std::ranges::sort(values);
    return values;
  };
  auto expected = c.at(kDecoded);
  auto actual = ProjectSchema(*parsed->value());
  expected[kIdentifierFieldIds] = sorted_ids(expected.at(kIdentifierFieldIds));
  actual[kIdentifierFieldIds] = sorted_ids(actual.at(kIdentifierFieldIds));
  std::vector<std::string> diffs;
  DiffJson(expected, actual, "", &diffs);
  return diffs;
}

class SchemaVerificationTest : public ::testing::TestWithParam<VerificationCase> {};

TEST_P(SchemaVerificationTest, Parse) {
  const auto& c = GetParam();
  if (!c.load_error.empty()) {
    FAIL() << c.load_error;
  }
  ReportOutcome(c, CheckCase(c.json));
}

INSTANTIATE_TEST_SUITE_P(Schema, SchemaVerificationTest,
                         ::testing::ValuesIn(LoadCases("schema", kSubdirs)), CaseName);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(SchemaVerificationTest);

}  // namespace

}  // namespace iceberg::verification
