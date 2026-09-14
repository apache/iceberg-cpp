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

#include <exception>
#include <string_view>

#include "iceberg/logging/log_macros.h"

namespace iceberg::internal {

// Isolate a Status-returning cleanup or reporting action so failures cannot
// change the transaction outcome or prevent subsequent actions from running.
template <typename Action>
void BestEffort(std::string_view name, Action&& action) noexcept {
  try {
    if (auto result = action(); !result) {
      ICEBERG_LOG_WARN("{} failed: {}", name, result.error().message);
    }
  } catch (const std::exception& e) {
    ICEBERG_LOG_WARN("{} threw: {}", name, e.what());
  } catch (...) {
    ICEBERG_LOG_WARN("{} threw an unknown exception", name);
  }
}

}  // namespace iceberg::internal
