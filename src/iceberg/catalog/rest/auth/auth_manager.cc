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

#include "iceberg/catalog/rest/auth/auth_manager.h"

#include "iceberg/catalog/rest/auth/auth_session.h"

namespace iceberg::rest::auth {

Result<std::unique_ptr<AuthSession>> AuthManager::InitSession(
    HttpClient& init_client,
    const std::unordered_map<std::string, std::string>& properties) {
  // By default, use the catalog session for initialization
  return CatalogSession(init_client, properties);
}

Result<std::unique_ptr<AuthSession>> AuthManager::TableSession(
    [[maybe_unused]] const TableIdentifier& table,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties,
    [[maybe_unused]] const AuthSession& parent) {
  // By default, return nullptr to indicate the parent session should be reused.
  return nullptr;
}

}  // namespace iceberg::rest::auth
