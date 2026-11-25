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

#include "iceberg/test/verification/verification_util.h"

#include <algorithm>
#include <cctype>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <utility>

#include "iceberg/result.h"
#include "iceberg/test/test_config.h"

namespace iceberg::verification {

namespace {

// Fixture fields read only by the loader and the outcome report.
constexpr std::string_view kCases = "cases";
constexpr std::string_view kNormativeLevel = "normative_level";

std::filesystem::path TableSpecDir() {
  return std::filesystem::path(ICEBERG_VERIFICATION_DIR) / "table-spec";
}

/// \brief Read the fixture file at `path` and return its `cases` array.
Result<nlohmann::json> ParseFixture(const std::filesystem::path& path) {
  std::ifstream file(path);
  if (!file) {
    return IOError("cannot open file");
  }
  nlohmann::json fixture;
  try {
    fixture = nlohmann::json::parse(file);
  } catch (const std::exception& e) {
    return JsonParseError("{}", e.what());
  }
  if (!fixture.contains(kCases) || !fixture[kCases].is_array()) {
    return JsonParseError("no `cases` array");
  }
  return std::move(fixture[kCases]);
}

std::string JoinDiffs(const std::vector<std::string>& diffs) {
  std::string out;
  for (const auto& diff : diffs) {
    out += "\n  ";
    out += diff;
  }
  return out;
}

}  // namespace

bool FixturesAvailable() { return std::filesystem::is_directory(TableSpecDir()); }

std::vector<VerificationCase> LoadCases(std::string_view surface,
                                        std::span<const std::string_view> subdirs) {
  std::vector<VerificationCase> cases;
  if (!FixturesAvailable()) {
    return cases;
  }
  for (auto subdir : subdirs) {
    auto load_error = [&](std::string id, const std::filesystem::path& path,
                          std::string_view message) {
      cases.push_back({.subdir = std::string(subdir),
                       .id = std::move(id),
                       .load_error = std::format("{}: {}", path.string(), message)});
    };
    auto path = TableSpecDir() / surface / subdir / "cases.json";
    auto fixture = ParseFixture(path);
    if (!fixture.has_value()) {
      load_error("load", path, fixture.error().message);
      continue;
    }
    auto& entries = *fixture;
    for (size_t i = 0; i < entries.size(); ++i) {
      auto& json = entries[i];
      if (!json.is_object() || !json.contains(kId) || !json[kId].is_string()) {
        load_error(std::format("cases-{}", i), path,
                   std::format("cases[{}] is not an object with a string `id`", i));
        continue;
      }
      cases.push_back({.subdir = std::string(subdir),
                       .id = json[kId].get<std::string>(),
                       .json = std::move(json)});
    }
  }
  return cases;
}

std::string CaseName(const ::testing::TestParamInfo<VerificationCase>& info) {
  std::string name = info.param.Label();
  for (auto& c : name) {
    if (!std::isalnum(static_cast<unsigned char>(c))) {
      c = '_';
    }
  }
  return name;
}

void DiffJson(const nlohmann::json& expected, const nlohmann::json& actual,
              const std::string& path, std::vector<std::string>* diffs) {
  if (expected.is_object() && actual.is_object()) {
    for (const auto& [key, value] : expected.items()) {
      auto child = path.empty() ? key : path + "." + key;
      if (!actual.contains(key)) {
        diffs->push_back(std::format("{}: missing, expected {}", child, value.dump()));
      } else {
        DiffJson(value, actual.at(key), child, diffs);
      }
    }
    for (const auto& [key, value] : actual.items()) {
      if (!expected.contains(key)) {
        auto child = path.empty() ? key : path + "." + key;
        diffs->push_back(std::format("{}: unexpected {}", child, value.dump()));
      }
    }
  } else if (expected.is_array() && actual.is_array()) {
    auto common = std::min(expected.size(), actual.size());
    for (size_t i = 0; i < common; ++i) {
      DiffJson(expected[i], actual[i], std::format("{}[{}]", path, i), diffs);
    }
    if (expected.size() != actual.size()) {
      diffs->push_back(std::format("{}: expected {} elements, got {}", path,
                                   expected.size(), actual.size()));
    }
  } else if (expected != actual) {
    diffs->push_back(std::format("{}: expected {}, got {}", path.empty() ? "value" : path,
                                 expected.dump(), actual.dump()));
  }
}

void ReportOutcome(const VerificationCase& c, const std::vector<std::string>& diffs) {
  const auto label = c.Label();
  if (diffs.empty()) {
    return;
  }
  if (c.json.value(kNormativeLevel, std::string("must")) == "should") {
    GTEST_SKIP() << "advisory_fail " << label << JoinDiffs(diffs);
  }
  for (const auto& diff : diffs) {
    ADD_FAILURE() << label << " " << diff;
  }
}

// The parameterized suites have no cases when the fixtures are absent. CMake checks
// the directory at configure time; this test catches it going missing afterwards,
// so the run fails instead of passing with no cases.
TEST(VerificationFixtures, Available) {
  ASSERT_TRUE(FixturesAvailable())
      << "iceberg-verification fixtures not found at " << ICEBERG_VERIFICATION_DIR
      << "; run `git submodule update --init testing/iceberg-verification`";
}

}  // namespace iceberg::verification
