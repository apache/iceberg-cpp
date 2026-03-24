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

#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/metrics.h"

namespace iceberg {
class Schema;
class MetricsConfig;
}  // namespace iceberg

namespace iceberg::avro {

/// \brief Utility class for computing Avro file metrics.
class ICEBERG_BUNDLE_EXPORT AvroMetrics {
 public:
  AvroMetrics() = delete;

  /// \brief Compute metrics from writer state.
  /// \param schema The Iceberg schema of the written data.
  /// \param num_records The number of records written.
  /// \param metrics_config The metrics configuration.
  /// \return Metrics for the written Avro file.
  static Metrics GetMetrics(const Schema& schema, int64_t num_records,
                            const MetricsConfig& metrics_config);
};

}  // namespace iceberg::avro
