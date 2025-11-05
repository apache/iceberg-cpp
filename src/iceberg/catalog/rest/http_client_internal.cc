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
#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/http_client_interal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

Result<std::unique_ptr<HttpClient>> HttpClient::Make(RestCatalogConfig config) {
  if (config.uri.empty()) {
    return InvalidArgument("HttpClient::Make received a config with empty URI");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto session_headers, config.GetExtraHeaders());

  auto config_ptr = std::make_shared<const RestCatalogConfig>(std::move(config));
  return std::unique_ptr<HttpClient>(
      new HttpClient(config_ptr, std::move(session_headers)));
}

HttpClient::HttpClient(std::shared_ptr<const RestCatalogConfig> config,
                       cpr::Header session_headers)
    : config(std::move(config)),
      base_url(config->uri),
      default_headers(std::move(session_headers)),
      session(std::make_unique<cpr::Session>()) {}

Result<cpr::Response> HttpClient::Get(const std::string& target,
                                      const cpr::Parameters& params,
                                      const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Get(); });
}

Result<cpr::Response> HttpClient::Post(const std::string& target, const cpr::Body& body,
                                       const cpr::Parameters& params,
                                       const cpr::Header& headers) {
  return Execute(target, params, headers, [&](cpr::Session& session) {
    session.SetBody(body);
    return session.Post();
  });
}

Result<cpr::Response> HttpClient::Head(const std::string& target,
                                       const cpr::Parameters& params,
                                       const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Head(); });
}

Result<cpr::Response> HttpClient::Delete(const std::string& target,
                                         const cpr::Parameters& params,
                                         const cpr::Header& headers) {
  return Execute(target, params, headers,
                 [&](cpr::Session& session) { return session.Delete(); });
}

template <typename Func>
Result<cpr::Response> HttpClient::Execute(const std::string& target,
                                          const cpr::Parameters& params,
                                          const cpr::Header& request_headers,
                                          Func&& perform_request) {
  cpr::Header combined_headers = default_headers;
  combined_headers.insert(request_headers.begin(), request_headers.end());

  session->SetUrl(base_url + target);
  session->SetParameters(params);
  session->SetHeader(combined_headers);

  cpr::Response response = perform_request(*session);
  return response;
}

}  // namespace iceberg::rest
