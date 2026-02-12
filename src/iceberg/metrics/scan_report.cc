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

ScanMetrics ScanMetrics::Of(MetricsContext& context) {
  ScanMetrics m;
  m.total_planning_duration = context.GetTimer("totalPlanningDuration");
  m.result_data_files = context.GetCounter("resultDataFiles");
  m.result_delete_files = context.GetCounter("resultDeleteFiles");
  m.scanned_data_manifests = context.GetCounter("scannedDataManifests");
  m.scanned_delete_manifests = context.GetCounter("scannedDeleteManifests");
  m.total_data_manifests = context.GetCounter("totalDataManifests");
  m.total_delete_manifests = context.GetCounter("totalDeleteManifests");
  m.total_file_size_in_bytes =
      context.GetCounter("totalFileSizeInBytes", CounterUnit::kBytes);
  m.total_delete_file_size_in_bytes =
      context.GetCounter("totalDeleteFileSizeInBytes", CounterUnit::kBytes);
  m.skipped_data_manifests = context.GetCounter("skippedDataManifests");
  m.skipped_delete_manifests = context.GetCounter("skippedDeleteManifests");
  m.skipped_data_files = context.GetCounter("skippedDataFiles");
  m.skipped_delete_files = context.GetCounter("skippedDeleteFiles");
  m.indexed_delete_files = context.GetCounter("indexedDeleteFiles");
  m.equality_delete_files = context.GetCounter("equalityDeleteFiles");
  m.positional_delete_files = context.GetCounter("positionalDeleteFiles");
  m.dvs = context.GetCounter("dvs");
  return m;
}

ScanMetrics ScanMetrics::Noop() { return ScanMetrics::Of(MetricsContext::Null()); }

ScanMetricsResult ScanMetrics::ToResult() const {
  ScanMetricsResult r;
  r.total_planning_duration = total_planning_duration
                                  ? TimerResult{total_planning_duration->Count(),
                                                total_planning_duration->TotalDuration()}
                                  : TimerResult{};
  r.result_data_files = result_data_files ? result_data_files->Value() : 0;
  r.result_delete_files = result_delete_files ? result_delete_files->Value() : 0;
  r.scanned_data_manifests = scanned_data_manifests ? scanned_data_manifests->Value() : 0;
  r.scanned_delete_manifests =
      scanned_delete_manifests ? scanned_delete_manifests->Value() : 0;
  r.total_data_manifests = total_data_manifests ? total_data_manifests->Value() : 0;
  r.total_delete_manifests = total_delete_manifests ? total_delete_manifests->Value() : 0;
  r.total_file_size_in_bytes =
      total_file_size_in_bytes ? total_file_size_in_bytes->Value() : 0;
  r.total_delete_file_size_in_bytes =
      total_delete_file_size_in_bytes ? total_delete_file_size_in_bytes->Value() : 0;
  r.skipped_data_manifests = skipped_data_manifests ? skipped_data_manifests->Value() : 0;
  r.skipped_delete_manifests =
      skipped_delete_manifests ? skipped_delete_manifests->Value() : 0;
  r.skipped_data_files = skipped_data_files ? skipped_data_files->Value() : 0;
  r.skipped_delete_files = skipped_delete_files ? skipped_delete_files->Value() : 0;
  r.indexed_delete_files = indexed_delete_files ? indexed_delete_files->Value() : 0;
  r.equality_delete_files = equality_delete_files ? equality_delete_files->Value() : 0;
  r.positional_delete_files =
      positional_delete_files ? positional_delete_files->Value() : 0;
  r.dvs = dvs ? dvs->Value() : 0;
  return r;
}

}  // namespace iceberg
