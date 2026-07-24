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

#include <memory>
#include <string>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/metrics/metrics_reporter.h"

/// \file iceberg/catalog/rest/rest_metrics_reporter.h
/// \brief MetricsReporter that POSTs reports to the Iceberg REST metrics endpoint.

namespace iceberg::rest {

/// \brief Reports scan and commit metrics to the Iceberg REST catalog metrics endpoint.
///
/// This is the default metrics reporter wired automatically by RestCatalog for each
/// table, mirroring Java's RESTMetricsReporter. It POSTs the serialized report to
/// POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics.
/// This C++ implementation calls HttpClient::Post() synchronously.
/// A future improvement would be to introduce a thread pool.
class ICEBERG_REST_EXPORT RestMetricsReporter : public MetricsReporter {
 public:
  /// \param client  Shared ownership of the HTTP client; must not be null.
  /// \param metrics_endpoint  Pre-built path from ResourcePaths::Metrics().
  /// \param session  Auth session used to authenticate the POST request.
  RestMetricsReporter(std::shared_ptr<HttpClientBase> client,
                      std::string metrics_endpoint,
                      std::shared_ptr<auth::AuthSession> session);

  /// \brief POST the report to the metrics endpoint, suppressing all errors.
  Status Report(const MetricsReport& report) override;

 private:
  /// \brief Build the JSON request body for a report, including the `report-type`
  /// field required by the REST metrics spec (not part of the core ToJson output).
  static Result<std::string> BuildRequestBody(const MetricsReport& report);

  std::shared_ptr<HttpClientBase> client_;
  std::string metrics_endpoint_;
  std::shared_ptr<auth::AuthSession> session_;
};

}  // namespace iceberg::rest
