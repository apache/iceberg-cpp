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

#include <functional>
#include <memory>
#include <string>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::rest {

using RestMetricsPost =
    std::function<void(const std::string&, const std::string&, auth::AuthSession&)>;

class ICEBERG_REST_EXPORT RestMetricsReporter final : public MetricsReporter {
 public:
  RestMetricsReporter(std::string metrics_endpoint,
                      std::shared_ptr<auth::AuthSession> session, Executor* executor,
                      RestMetricsPost post);

  Status Report(const MetricsReport& report) override;

 private:
  static Result<std::string> BuildRequestBody(const MetricsReport& report);

  std::string metrics_endpoint_;
  std::shared_ptr<auth::AuthSession> session_;
  Executor* executor_;
  RestMetricsPost post_;
};

}  // namespace iceberg::rest
