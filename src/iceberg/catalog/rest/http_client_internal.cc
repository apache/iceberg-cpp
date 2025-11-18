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

#include <nlohmann/json.hpp>

#include "cpr/body.h"
#include "cpr/cprtypes.h"
#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/http_client_interal.h"

namespace iceberg::rest {

HttpClient::HttpClient(const RestCatalogConfig& config)
    : default_headers_{config.GetExtraHeaders().begin(), config.GetExtraHeaders().end()},
      session_{std::make_unique<cpr::Session>()} {}

Result<HttpResponse> HttpClient::Get(const std::string& target,
                                     const cpr::Parameters& params,
                                     const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Get(); });
}

Result<HttpResponse> HttpClient::Post(const std::string& target, const cpr::Body& body,
                                      const cpr::Parameters& params,
                                      const cpr::Header& headers) {
  return Execute(target, params, headers, [&](cpr::Session& session) {
    session.SetBody(body);
    return session.Post();
  });
}

Result<HttpResponse> HttpClient::Head(const std::string& target,
                                      const cpr::Parameters& params,
                                      const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Head(); });
}

Result<HttpResponse> HttpClient::Delete(const std::string& target,
                                        const cpr::Parameters& params,
                                        const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Delete(); });
}

}  // namespace iceberg::rest
