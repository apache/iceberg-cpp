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

#pragma once

/// \file iceberg/test/verification/verification_util.h
/// Helpers for running the apache/iceberg-verification conformance fixtures.
///
/// Each surface under `table-spec/` holds one `cases.json` per subdirectory. A test
/// subscribes to subdirectories by name, runs every case through the public API, and
/// reports differences labeled `<subdir>/<case-id> <path>`, as the surface READMEs
/// require.

#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace iceberg::verification {

/// \name Fields of a fixture `cases.json` and its case objects.
/// @{
inline constexpr std::string_view kId = "id";
inline constexpr std::string_view kInput = "input";
inline constexpr std::string_view kValid = "valid";
inline constexpr std::string_view kDecoded = "decoded";
/// @}

/// \name Fields of the `decoded` projections shared by the schema and types surfaces.
/// @{
inline constexpr std::string_view kType = "type";
inline constexpr std::string_view kName = "name";
inline constexpr std::string_view kRequired = "required";
inline constexpr std::string_view kFields = "fields";
inline constexpr std::string_view kPrecision = "precision";
inline constexpr std::string_view kScale = "scale";
inline constexpr std::string_view kLength = "length";
/// @}

/// \brief A case from a fixture `cases.json`, or a fixture that failed to load.
struct VerificationCase {
  /// \brief Subdirectory of the surface, e.g. `primitive`.
  std::string subdir;
  /// \brief Case id, unique within the surface.
  std::string id;
  /// \brief The case object as written in the fixture.
  nlohmann::json json;
  /// \brief Set when the fixture could not be loaded; the case then fails with it.
  std::string load_error;

  /// \brief `<subdir>/<id>`, the label the fixture READMEs use.
  std::string Label() const { return subdir + "/" + id; }
};

/// \brief Print a case as its label in GTest output.
inline void PrintTo(const VerificationCase& c, std::ostream* os) { *os << c.Label(); }

/// \brief Whether the fixture checkout at ICEBERG_VERIFICATION_DIR is present.
bool FixturesAvailable();

/// \brief Load the cases of the given subdirectories of `table-spec/<surface>/`.
///
/// Returns no cases when the fixtures are absent. A subscribed subdirectory whose
/// `cases.json` is missing or malformed yields one case carrying the load error, so
/// the problem fails a test instead of silently dropping cases.
std::vector<VerificationCase> LoadCases(std::string_view surface,
                                        std::span<const std::string_view> subdirs);

/// \brief GTest parameter name for a case, derived from its label.
std::string CaseName(const ::testing::TestParamInfo<VerificationCase>& info);

/// \brief Append every difference between `expected` and `actual` to `diffs`.
///
/// Each difference is labeled with its path, e.g. `fields[2].parent`.
void DiffJson(const nlohmann::json& expected, const nlohmann::json& actual,
              const std::string& path, std::vector<std::string>* diffs);

/// \brief Turn the differences found for a case into a test outcome.
///
/// No differences pass. Differences fail the test, unless the case is advisory
/// (`normative_level: should`), in which case the test is skipped with the
/// differences in the message.
void ReportOutcome(const VerificationCase& c, const std::vector<std::string>& diffs);

}  // namespace iceberg::verification
