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

#include "iceberg/catalog/rest/auth/auth_session.h"

#include <utility>

namespace iceberg::rest::auth {

namespace {

/// \brief Default implementation that adds static headers to requests.
class DefaultAuthSession : public AuthSession {
 public:
  explicit DefaultAuthSession(std::unordered_map<std::string, std::string> headers)
      : headers_(std::move(headers)) {}

  Status Authenticate(std::unordered_map<std::string, std::string>& headers) override {
    for (const auto& [key, value] : headers_) {
      headers.try_emplace(key, value);
    }
    return {};
  }

 private:
  std::unordered_map<std::string, std::string> headers_;
};

}  // namespace

std::shared_ptr<AuthSession> AuthSession::MakeDefault(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

}  // namespace iceberg::rest::auth
