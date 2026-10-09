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

// Compiles the public API as a C++20 consumer in compatibility mode.

#include <concepts>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "iceberg/cpp20_compatibility_internal.h"

static_assert(ICEBERG_COMPILER_CPLUSPLUS == 202002L,
              "cpp20_compatibility_test must build as C++20");

static_assert(iceberg::ByteSwap(uint32_t{0x12345678}) == uint32_t{0x78563412});
static_assert(noexcept(::iceberg::unreachable()));
static_assert(std::same_as<decltype(::iceberg::unreachable()), void>);

class UserBuilder : public iceberg::ErrorCollector {
 public:
  decltype(auto) Validate() { return AddError(iceberg::InvalidArgument("bad")); }
};

using SnapshotSetterResult = iceberg::SnapshotUpdate&;
using ErrorAdderResult = iceberg::ErrorCollector&;
using UserBuilderResult = iceberg::ErrorCollector&;

static_assert(
    std::same_as<decltype(std::declval<UserBuilder&>().Validate()), UserBuilderResult>);

static_assert(
    std::same_as<
        decltype(std::declval<iceberg::FastAppend&>()
                     .StageOnly()
                     .ReportWith(
                         std::declval<std::shared_ptr<iceberg::MetricsReporter>>())
                     .DeleteWith(std::declval<
                                 std::function<iceberg::Status(const std::string&)>>())
                     .ScanManifestsWith(std::declval<iceberg::Executor&>())
                     .ToBranch("branch")
                     .Set("key", "value")
                     .WriteManifestsWith(std::declval<iceberg::Executor&>(), 2)),
        SnapshotSetterResult>);

static_assert(
    std::same_as<decltype(std::declval<iceberg::FastAppend&>()
                              .AddError(std::declval<iceberg::Error>())
                              .AddError(iceberg::ErrorKind::kInvalidArgument, "bad {}", 1)
                              .AddError(iceberg::InvalidArgument("bad"))),
                 ErrorAdderResult>);

int main() {
  UserBuilder builder;
  builder.Validate();
  return builder.error_count() == 1 ? 0 : 1;
}
