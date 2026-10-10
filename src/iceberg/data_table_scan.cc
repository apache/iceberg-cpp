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

#include "iceberg/data_table_scan.h"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <optional>
#include <utility>

#include "iceberg/expression/sanitize_expression.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"

namespace iceberg {

namespace {

template <typename T>
class EmptyStream final : public Stream<T> {
 public:
  Result<std::optional<T>> NextImpl() override { return std::nullopt; }
};

Result<ScanReport> MakeScanReport(const DataTableScan& scan, const Snapshot& snapshot,
                                  ScanMetricsResult scan_metrics) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema_ptr, scan.schema());

  ICEBERG_ASSIGN_OR_RAISE(
      auto projected_id_set,
      GetProjectedIdsVisitor::GetProjectedIds(*schema_ptr, /*include_struct_ids=*/true));
  std::vector<int32_t> projected_field_ids(projected_id_set.begin(),
                                           projected_id_set.end());
  std::ranges::sort(projected_field_ids);

  std::vector<std::string> projected_field_names;
  projected_field_names.reserve(projected_field_ids.size());
  for (int32_t field_id : projected_field_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_name, schema_ptr->FindColumnNameById(field_id));
    ICEBERG_CHECK(field_name.has_value(), "Projected field {} not found in schema",
                  field_id);
    projected_field_names.emplace_back(*field_name);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto sanitized_filter,
                          SanitizeExpression::Sanitize(*schema_ptr, scan.filter(),
                                                       scan.context().case_sensitive));

  return ScanReport{
      .table_name = scan.context().table_name,
      .snapshot_id = snapshot.snapshot_id,
      .filter = std::move(sanitized_filter),
      .schema_id = schema_ptr->schema_id(),
      .projected_field_ids = std::move(projected_field_ids),
      .projected_field_names = std::move(projected_field_names),
      .scan_metrics = std::move(scan_metrics),
      .metadata = scan.context().options,
  };
}

class ReportingFileTaskStream final : public FileScanTaskStream {
 public:
  ReportingFileTaskStream(FileScanTaskStreamPtr stream,
                          std::shared_ptr<ScanMetrics> scan_metrics,
                          std::chrono::nanoseconds planning_duration,
                          std::shared_ptr<MetricsReporter> reporter, ScanReport report)
      : stream_(std::move(stream)),
        scan_metrics_(std::move(scan_metrics)),
        planning_duration_(std::move(planning_duration)),
        reporter_(std::move(reporter)),
        report_(std::move(report)) {}

  ~ReportingFileTaskStream() override { Finalize(); }

  Result<std::optional<std::shared_ptr<FileScanTask>>> NextImpl() override {
    auto start = std::chrono::steady_clock::now();
    auto result = stream_->Next();
    planning_duration_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start);
    if (!result.has_value()) {
      // Failed planning does not emit a successful scan report.
      finalized_ = true;
    } else if (!result.value().has_value()) {
      Finalize();
    }
    return result;
  }

 private:
  void Finalize() {
    if (finalized_) {
      return;
    }
    finalized_ = true;
    scan_metrics_->total_planning_duration->Record(planning_duration_);
    report_.scan_metrics = scan_metrics_->ToResult();
    std::ignore = reporter_->Report(report_);
  }

  FileScanTaskStreamPtr stream_;
  std::shared_ptr<ScanMetrics> scan_metrics_;
  std::chrono::nanoseconds planning_duration_;
  std::shared_ptr<MetricsReporter> reporter_;
  ScanReport report_;
  bool finalized_ = false;
};

}  // namespace

Result<std::unique_ptr<DataTableScan>> DataTableScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<DataTableScan>(new DataTableScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> DataTableScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(auto stream, PlanFilesStream());
  return stream->ToVector();
}

Result<FileScanTaskStreamPtr> DataTableScan::PlanFilesStream() const {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, this->snapshot());
  if (!snapshot) {
    return std::make_unique<EmptyStream<std::shared_ptr<FileScanTask>>>();
  }

  std::shared_ptr<ScanMetrics> scan_metrics;
  std::optional<std::chrono::steady_clock::time_point> planning_start;
  if (context_.metrics_reporter) {
    auto metrics_context = MetricsContext::Default();
    scan_metrics = ScanMetrics::Make(*metrics_context);
    planning_start = std::chrono::steady_clock::now();
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  SnapshotReader snapshot_reader(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto data_manifests, snapshot_reader.DataManifests(io_));
  ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests, snapshot_reader.DeleteManifests(io_));

  if (scan_metrics) {
    scan_metrics->total_data_manifests->Increment(
        static_cast<int64_t>(data_manifests.size()));
    scan_metrics->total_delete_manifests->Increment(
        static_cast<int64_t>(delete_manifests.size()));
  }

  std::vector<ManifestFile> owned_data_manifests(
      std::make_move_iterator(data_manifests.begin()),
      std::make_move_iterator(data_manifests.end()));
  std::vector<ManifestFile> owned_delete_manifests(
      std::make_move_iterator(delete_manifests.begin()),
      std::make_move_iterator(delete_manifests.end()));

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(io_, schema_, specs_by_id, std::move(owned_data_manifests),
                          std::move(owned_delete_manifests)));
  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .IgnoreDeleted()
      .ColumnsToKeepStats(context_.columns_to_keep_stats)
      .WithScanMetrics(scan_metrics);
  if (data_manifests.size() > 1 || delete_manifests.size() > 1) {
    manifest_group->PlanWith(context_.plan_executor);
  }
  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }

  ICEBERG_ASSIGN_OR_RAISE(auto stream, std::move(*manifest_group).PlanFilesStream());
  if (!planning_start.has_value()) {
    return stream;
  }

  auto planning_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - planning_start.value());

  auto report = MakeScanReport(*this, *snapshot, ScanMetricsResult{});
  if (!report.has_value()) {
    // Scan reporting is best effort.
    return stream;
  }

  return std::make_unique<ReportingFileTaskStream>(
      std::move(stream), std::move(scan_metrics), planning_duration,
      context_.metrics_reporter, std::move(report).value());
}

}  // namespace iceberg
