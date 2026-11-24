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

// Runs the `table-spec/partition-transforms` surface: binding `input.transform` to
// `input.source_type` yields one of `decoded.result_types`, and applying it to each
// of `input.values` yields the value at the same index of `decoded.values`.

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <format>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/result.h"
#include "iceberg/test/verification/verification_util.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/string_util.h"

namespace iceberg::verification {

namespace {

constexpr std::array<std::string_view, 8> kSubdirs = {
    "identity", "bucket", "truncate", "year", "month", "day", "hour", "void"};

constexpr std::string_view kTransform = "transform";
constexpr std::string_view kSourceType = "source_type";
constexpr std::string_view kValues = "values";
constexpr std::string_view kResultTypes = "result_types";

/// \brief Decode a language-neutral value of this surface into a literal of `type`.
Result<Literal> DecodeValue(const nlohmann::json& json,
                            const std::shared_ptr<PrimitiveType>& type) {
  if (json.is_null()) {
    return Literal::Null(type);
  }
  switch (type->type_id()) {
    case TypeId::kLong: {
      // partition-transforms/README.md: "`long` values are decimal strings so JSON
      // number handling cannot lose 64-bit precision."
      if (!json.is_string()) {
        return JsonParseError("long value {} is not a decimal string", json.dump());
      }
      ICEBERG_ASSIGN_OR_RAISE(auto value,
                              StringUtils::ParseNumber<int64_t>(json.get<std::string>()));
      return Literal::Long(value);
    }
    default:
      return LiteralFromJson(json, type.get());
  }
}

std::string Describe(const Literal& literal) {
  if (literal.IsNull()) {
    return "null";
  }
  auto json = ToJson(literal);
  return json.has_value() ? json->dump() : literal.ToString();
}

Result<std::shared_ptr<PrimitiveType>> ParsePrimitiveType(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<Type> type, TypeFromJson(json));
  if (!type->is_primitive()) {
    return InvalidArgument("{} is not a primitive type", json.dump());
  }
  return std::static_pointer_cast<PrimitiveType>(type);
}

std::vector<std::string> CheckCase(const nlohmann::json& c) {
  const auto& input = c.at(kInput);
  const auto& decoded = c.at(kDecoded);

  auto source_type = ParsePrimitiveType(input.at(kSourceType));
  if (!source_type.has_value()) {
    return {std::format("source type: {}", source_type.error().message)};
  }
  auto transform = TransformFromString(input.at(kTransform).get<std::string>());
  if (!transform.has_value()) {
    return {std::format("transform: {}", transform.error().message)};
  }
  auto bound = (*transform)->Bind(*source_type);
  if (!bound.has_value()) {
    return {std::format("bind failed: {}", bound.error().message)};
  }

  std::vector<std::string> diffs;
  auto result_type = (*bound)->ResultType();
  const auto& allowed = decoded.at(kResultTypes);
  bool result_type_allowed = std::ranges::any_of(allowed, [&](const auto& json) {
    auto type = TypeFromJson(json);
    return type.has_value() && **type == *result_type;
  });
  if (!result_type_allowed) {
    diffs.push_back(std::format("result_type: expected one of {}, got {}", allowed.dump(),
                                result_type->ToString()));
  }
  if (!result_type->is_primitive()) {
    diffs.push_back(
        std::format("result_type: {} is not primitive", result_type->ToString()));
    return diffs;
  }
  auto result_primitive = std::static_pointer_cast<PrimitiveType>(result_type);

  const auto& inputs = input.at(kValues);
  const auto& expected_values = decoded.at(kValues);
  if (inputs.size() != expected_values.size()) {
    diffs.push_back(std::format("values: {} inputs but {} expected values", inputs.size(),
                                expected_values.size()));
    return diffs;
  }
  for (size_t i = 0; i < inputs.size(); ++i) {
    auto label = std::format("values[{}]", i);
    auto value = DecodeValue(inputs[i], *source_type);
    if (!value.has_value()) {
      diffs.push_back(std::format("{}: cannot decode input {}: {}", label,
                                  inputs[i].dump(), value.error().message));
      continue;
    }
    Result<Literal> actual = Literal::Null(result_primitive);
    try {
      actual = (*bound)->Transform(*value);
    } catch (const std::exception& e) {
      diffs.push_back(std::format("{}: transform threw: {}", label, e.what()));
      continue;
    }
    if (!actual.has_value()) {
      diffs.push_back(
          std::format("{}: transform failed: {}", label, actual.error().message));
      continue;
    }
    if (expected_values[i].is_null()) {
      if (!actual->IsNull()) {
        diffs.push_back(
            std::format("{}: expected null, got {}", label, Describe(*actual)));
      }
      continue;
    }
    auto expected = DecodeValue(expected_values[i], result_primitive);
    if (!expected.has_value()) {
      diffs.push_back(std::format("{}: cannot decode expected {} as {}: {}", label,
                                  expected_values[i].dump(), result_type->ToString(),
                                  expected.error().message));
      continue;
    }
    if (actual->IsNull() || *actual != *expected) {
      diffs.push_back(std::format("{}: expected {}, got {}", label,
                                  expected_values[i].dump(), Describe(*actual)));
    }
  }
  return diffs;
}

class PartitionTransformsVerificationTest
    : public ::testing::TestWithParam<VerificationCase> {};

TEST_P(PartitionTransformsVerificationTest, Apply) {
  const auto& c = GetParam();
  if (!c.load_error.empty()) {
    FAIL() << c.load_error;
  }
  ReportOutcome(c, CheckCase(c.json));
}

INSTANTIATE_TEST_SUITE_P(PartitionTransforms, PartitionTransformsVerificationTest,
                         ::testing::ValuesIn(LoadCases("partition-transforms", kSubdirs)),
                         CaseName);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(PartitionTransformsVerificationTest);

}  // namespace

}  // namespace iceberg::verification
