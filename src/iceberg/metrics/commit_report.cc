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

#include "iceberg/metrics/commit_report.h"

namespace iceberg {

CommitMetrics CommitMetrics::Of(MetricsContext& context) {
  CommitMetrics m;
  m.total_duration = context.GetTimer("totalDuration");
  m.attempts = context.GetCounter("attempts", CounterUnit::kCount);
  return m;
}

CommitMetrics CommitMetrics::Noop() { return CommitMetrics::Of(MetricsContext::Null()); }

void CommitMetrics::PopulateResult(CommitMetricsResult& result) const {
  result.total_duration =
      total_duration ? TimerResult{.count = total_duration->Count(),
                                   .total_duration = total_duration->TotalDuration()}
                     : TimerResult{};
  result.attempts = attempts ? attempts->Value() : 0;
}

}  // namespace iceberg
