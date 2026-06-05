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

#include "iceberg/metrics/scan_report.h"

namespace iceberg {

std::unique_ptr<ScanMetrics> ScanMetrics::Make(MetricsContext& context) {
  auto m = std::unique_ptr<ScanMetrics>(new ScanMetrics());
  m->total_planning_duration =
      context.GetTimer("total-planning-duration", TimerUnit::kNanoseconds);
  m->result_data_files = context.GetCounter("result-data-files");
  m->result_delete_files = context.GetCounter("result-delete-files");
  m->scanned_data_manifests = context.GetCounter("scanned-data-manifests");
  m->scanned_delete_manifests = context.GetCounter("scanned-delete-manifests");
  m->total_data_manifests = context.GetCounter("total-data-manifests");
  m->total_delete_manifests = context.GetCounter("total-delete-manifests");
  m->total_file_size_in_bytes =
      context.GetCounter("total-file-size-in-bytes", CounterUnit::kBytes);
  m->total_delete_file_size_in_bytes =
      context.GetCounter("total-delete-file-size-in-bytes", CounterUnit::kBytes);
  m->skipped_data_manifests = context.GetCounter("skipped-data-manifests");
  m->skipped_delete_manifests = context.GetCounter("skipped-delete-manifests");
  m->skipped_data_files = context.GetCounter("skipped-data-files");
  m->skipped_delete_files = context.GetCounter("skipped-delete-files");
  m->indexed_delete_files = context.GetCounter("indexed-delete-files");
  m->equality_delete_files = context.GetCounter("equality-delete-files");
  m->positional_delete_files = context.GetCounter("positional-delete-files");
  m->dvs = context.GetCounter("dvs");
  return m;
}

std::unique_ptr<ScanMetrics> ScanMetrics::Noop() {
  return ScanMetrics::Make(*MetricsContext::Noop());
}

ScanMetricsResult ScanMetrics::ToResult() const {
  ScanMetricsResult r;
  auto snap = [](const std::shared_ptr<Counter>& c) -> std::optional<CounterResult> {
    if (!c || c->IsNoop()) return std::nullopt;
    return CounterResult{.unit = c->unit(), .value = c->value()};
  };

  if (total_planning_duration && !total_planning_duration->IsNoop()) {
    r.total_planning_duration =
        TimerResult{.unit = std::string(total_planning_duration->Unit()),
                    .count = total_planning_duration->Count(),
                    .total_duration = total_planning_duration->TotalDuration()};
  }
  r.result_data_files = snap(result_data_files);
  r.result_delete_files = snap(result_delete_files);
  r.scanned_data_manifests = snap(scanned_data_manifests);
  r.scanned_delete_manifests = snap(scanned_delete_manifests);
  r.total_data_manifests = snap(total_data_manifests);
  r.total_delete_manifests = snap(total_delete_manifests);
  r.total_file_size_in_bytes = snap(total_file_size_in_bytes);
  r.total_delete_file_size_in_bytes = snap(total_delete_file_size_in_bytes);
  r.skipped_data_manifests = snap(skipped_data_manifests);
  r.skipped_delete_manifests = snap(skipped_delete_manifests);
  r.skipped_data_files = snap(skipped_data_files);
  r.skipped_delete_files = snap(skipped_delete_files);
  r.indexed_delete_files = snap(indexed_delete_files);
  r.equality_delete_files = snap(equality_delete_files);
  r.positional_delete_files = snap(positional_delete_files);
  r.dvs = snap(dvs);
  return r;
}

ScanMetricsResult ScanMetricsResult::From(const ScanMetrics& scan_metrics) {
  return scan_metrics.ToResult();
}

}  // namespace iceberg
