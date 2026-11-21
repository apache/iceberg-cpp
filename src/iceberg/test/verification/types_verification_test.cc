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

// Runs the `table-spec/types` surface: parse(input) == decoded, and where the case
// carries `canonical`, re-serializing the parsed type reproduces it byte for byte.

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
#include "iceberg/schema_field.h"
#include "iceberg/test/verification/verification_util.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::verification {

namespace {

constexpr std::array<std::string_view, 4> kSubdirs = {"primitive", "nested", "variant",
                                                      "geospatial"};

// Fields specific to this surface: the case's `canonical` and the rest from the
// `decoded` projection.
constexpr std::string_view kCanonical = "canonical";
constexpr std::string_view kCrs = "crs";
constexpr std::string_view kAlgorithm = "algorithm";
constexpr std::string_view kElementId = "element-id";
constexpr std::string_view kElementRequired = "element-required";
constexpr std::string_view kElement = "element";
constexpr std::string_view kKeyId = "key-id";
constexpr std::string_view kKey = "key";
constexpr std::string_view kValueId = "value-id";
constexpr std::string_view kValueRequired = "value-required";
constexpr std::string_view kValue = "value";

/// \brief Map a type to the language-neutral `decoded` shape of the types surface.
nlohmann::json ProjectType(const Type& type) {
  nlohmann::json json{{kType, std::string(ToString(type.type_id()))}};
  switch (type.type_id()) {
    case TypeId::kDecimal: {
      const auto& decimal = internal::checked_cast<const DecimalType&>(type);
      json[kPrecision] = decimal.precision();
      json[kScale] = decimal.scale();
      break;
    }
    case TypeId::kFixed:
      json[kLength] = internal::checked_cast<const FixedType&>(type).length();
      break;
    case TypeId::kGeometry:
      json[kCrs] = std::string(internal::checked_cast<const GeometryType&>(type).crs());
      break;
    case TypeId::kGeography: {
      const auto& geography = internal::checked_cast<const GeographyType&>(type);
      json[kCrs] = std::string(geography.crs());
      json[kAlgorithm] = std::string(ToString(geography.algorithm()));
      break;
    }
    case TypeId::kStruct: {
      json[kFields] = nlohmann::json::array();
      for (const auto& field : internal::checked_cast<const StructType&>(type).fields()) {
        json[kFields].push_back({{kId, field.field_id()},
                                 {kName, std::string(field.name())},
                                 {kRequired, !field.optional()},
                                 {kType, ProjectType(*field.type())}});
      }
      break;
    }
    case TypeId::kList: {
      const auto& element = internal::checked_cast<const ListType&>(type).element();
      json[kElementId] = element.field_id();
      json[kElementRequired] = !element.optional();
      json[kElement] = ProjectType(*element.type());
      break;
    }
    case TypeId::kMap: {
      const auto& map = internal::checked_cast<const MapType&>(type);
      json[kKeyId] = map.key().field_id();
      json[kKey] = ProjectType(*map.key().type());
      json[kValueId] = map.value().field_id();
      json[kValueRequired] = !map.value().optional();
      json[kValue] = ProjectType(*map.value().type());
      break;
    }
    default:
      break;
  }
  return json;
}

std::vector<std::string> CheckCase(const nlohmann::json& c) {
  std::optional<Result<std::unique_ptr<Type>>> parsed;
  try {
    parsed = TypeFromJson(c.at(kInput));
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

  const auto& type = *parsed->value();
  std::vector<std::string> diffs;
  DiffJson(c.at(kDecoded), ProjectType(type), "", &diffs);
  if (c.contains(kCanonical)) {
    auto written = ToJson(type);
    if (!written.has_value()) {
      diffs.push_back(std::format("serialize failed: {}", written.error().message));
    } else {
      DiffJson(c[kCanonical], *written, std::string(kCanonical), &diffs);
    }
  }
  return diffs;
}

class TypesVerificationTest : public ::testing::TestWithParam<VerificationCase> {};

TEST_P(TypesVerificationTest, Parse) {
  const auto& c = GetParam();
  if (!c.load_error.empty()) {
    FAIL() << c.load_error;
  }
  ReportOutcome(c, CheckCase(c.json));
}

INSTANTIATE_TEST_SUITE_P(Types, TypesVerificationTest,
                         ::testing::ValuesIn(LoadCases("types", kSubdirs)), CaseName);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(TypesVerificationTest);

}  // namespace

}  // namespace iceberg::verification
