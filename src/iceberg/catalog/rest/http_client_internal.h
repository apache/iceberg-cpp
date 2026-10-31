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

#include <chrono>
#include <cstdint>
#include <ctime>
#include <functional>
#include <optional>

#include <cpr/response.h>

#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"

namespace iceberg::rest::detail {

ICEBERG_REST_EXPORT bool ShouldRetry(
    const cpr::Response& response, const HttpRequest& request, int64_t exec_count,
    const HttpClientOptions& options,
    std::optional<std::chrono::milliseconds> elapsed = std::nullopt,
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now());

ICEBERG_REST_EXPORT std::chrono::milliseconds RetryInterval(
    const cpr::Response& response, int64_t exec_count,
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now());

/// Internal timing seam for testing the retry execution loop without real waits.
struct ICEBERG_REST_EXPORT RetryTiming {
  RetryTiming();
  std::function<std::chrono::system_clock::time_point()> now;
  std::function<void(std::chrono::milliseconds)> sleep;
};

struct Attempt {
  cpr::Response response;
  bool retry_approved = false;
};

using SendAttempt =
    std::function<Result<Attempt>(int64_t, std::chrono::system_clock::time_point)>;

ICEBERG_REST_EXPORT Result<cpr::Response> SendWithRetry(const HttpClientOptions& options,
                                                        const HttpRequest& request,
                                                        const SendAttempt& send,
                                                        const RetryTiming& timing = {});

/// Tracks transfer inactivity independently of the average byte rate.
class ICEBERG_REST_EXPORT SocketIdleTimeout {
 public:
  using Clock = std::chrono::steady_clock;

  explicit SocketIdleTimeout(std::chrono::milliseconds timeout) : timeout_(timeout) {}

  bool Check(int64_t downloaded, int64_t uploaded, int64_t header_bytes,
             Clock::time_point now = Clock::now());
  void Reset() { last_progress_.reset(); }
  void RecordReadActivity(Clock::time_point now = Clock::now()) { last_progress_ = now; }
  std::optional<std::chrono::milliseconds> Remaining(
      Clock::time_point now = Clock::now()) const;

 private:
  std::chrono::milliseconds timeout_;
  std::optional<Clock::time_point> last_progress_;
  int64_t downloaded_ = 0;
  int64_t uploaded_ = 0;
  int64_t header_bytes_ = 0;
};

}  // namespace iceberg::rest::detail
