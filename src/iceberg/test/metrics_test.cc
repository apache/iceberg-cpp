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

#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/json_serde.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

TEST(DefaultCounterTest, IncrementByOne) {
  DefaultCounter c;
  EXPECT_EQ(c.Value(), 0);
  c.Increment();
  EXPECT_EQ(c.Value(), 1);
}

TEST(DefaultCounterTest, IncrementByAmount) {
  DefaultCounter c;
  c.Increment(42);
  EXPECT_EQ(c.Value(), 42);
  c.Increment(8);
  EXPECT_EQ(c.Value(), 50);
}

TEST(DefaultCounterTest, UnitCount) {
  DefaultCounter c(CounterUnit::kCount);
  EXPECT_EQ(c.Unit(), CounterUnit::kCount);
  EXPECT_FALSE(c.IsNoop());
}

TEST(DefaultCounterTest, UnitBytes) {
  DefaultCounter c(CounterUnit::kBytes);
  EXPECT_EQ(c.Unit(), CounterUnit::kBytes);
}

TEST(NoopCounterTest, IsNoopAndAlwaysZero) {
  Counter& noop = Counter::Noop();
  EXPECT_TRUE(noop.IsNoop());
  noop.Increment();
  noop.Increment(100);
  EXPECT_EQ(noop.Value(), 0);
}

// ---------------------------------------------------------------------------
// Timer
// ---------------------------------------------------------------------------

TEST(DefaultTimerTest, RaiiRecordsOnce) {
  DefaultTimer t;
  EXPECT_EQ(t.Count(), 0);
  {
    auto timed = t.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(t.Count(), 1);
  EXPECT_GT(t.TotalDuration().count(), 0);
}

TEST(DefaultTimerTest, ExplicitStopRecordsOnce) {
  DefaultTimer t;
  auto timed = t.Start();
  timed.Stop();
  EXPECT_EQ(t.Count(), 1);
  // Destructor must not double-record.
}

TEST(DefaultTimerTest, RecordDirect) {
  DefaultTimer t;
  t.Record(std::chrono::nanoseconds{1000});
  t.Record(std::chrono::nanoseconds{500});
  EXPECT_EQ(t.Count(), 2);
  EXPECT_EQ(t.TotalDuration(), std::chrono::nanoseconds{1500});
}

TEST(DefaultTimerTest, MoveDoesNotDoubleRecord) {
  DefaultTimer t;
  {
    auto a = t.Start();
    auto b = std::move(a);  // a is moved-from; destructor must not record
  }  // b records exactly once on destruction
  EXPECT_EQ(t.Count(), 1);
}

TEST(NoopTimerTest, IsNoopAndAlwaysZero) {
  Timer& noop = Timer::Noop();
  EXPECT_TRUE(noop.IsNoop());
  {
    auto timed = noop.Start();
  }
  EXPECT_EQ(noop.Count(), 0);
  EXPECT_EQ(noop.TotalDuration().count(), 0);
}

// ---------------------------------------------------------------------------
// MetricsContext
// ---------------------------------------------------------------------------

TEST(DefaultMetricsContextTest, SameNameReturnsSameObject) {
  DefaultMetricsContext ctx;
  auto c1 = ctx.GetCounter("foo");
  auto c2 = ctx.GetCounter("foo");
  EXPECT_EQ(c1.get(), c2.get());
}

TEST(DefaultMetricsContextTest, DifferentNamesReturnDifferentObjects) {
  DefaultMetricsContext ctx;
  auto c1 = ctx.GetCounter("a");
  auto c2 = ctx.GetCounter("b");
  EXPECT_NE(c1.get(), c2.get());
}

TEST(DefaultMetricsContextTest, TimerSameNameReturnsSameObject) {
  DefaultMetricsContext ctx;
  auto t1 = ctx.GetTimer("dur");
  auto t2 = ctx.GetTimer("dur");
  EXPECT_EQ(t1.get(), t2.get());
}

TEST(NullMetricsContextTest, ReturnsNoopInstances) {
  MetricsContext& null_ctx = MetricsContext::Null();
  EXPECT_TRUE(null_ctx.GetCounter("x")->IsNoop());
  EXPECT_TRUE(null_ctx.GetTimer("y")->IsNoop());
}

TEST(NullMetricsContextTest, ReturnsSameSharedPtrEachCall) {
  // Verify the static-shared_ptr fix: no new control block per call.
  MetricsContext& null_ctx = MetricsContext::Null();
  auto c1 = null_ctx.GetCounter("a");
  auto c2 = null_ctx.GetCounter("b");
  EXPECT_EQ(c1.get(), c2.get());  // same noop singleton
  auto t1 = null_ctx.GetTimer("x");
  auto t2 = null_ctx.GetTimer("y");
  EXPECT_EQ(t1.get(), t2.get());
}

// ---------------------------------------------------------------------------
// ScanMetrics
// ---------------------------------------------------------------------------

TEST(ScanMetricsTest, NoopToResultIsAllZero) {
  auto r = ScanMetrics::Noop().ToResult();
  ScanMetricsResult expected{};
  EXPECT_EQ(r.total_planning_duration.count, expected.total_planning_duration.count);
  EXPECT_EQ(r.total_planning_duration.total_duration,
            expected.total_planning_duration.total_duration);
  EXPECT_EQ(r.result_data_files, expected.result_data_files);
  EXPECT_EQ(r.result_delete_files, expected.result_delete_files);
  EXPECT_EQ(r.scanned_data_manifests, expected.scanned_data_manifests);
  EXPECT_EQ(r.scanned_delete_manifests, expected.scanned_delete_manifests);
  EXPECT_EQ(r.total_data_manifests, expected.total_data_manifests);
  EXPECT_EQ(r.total_delete_manifests, expected.total_delete_manifests);
  EXPECT_EQ(r.total_file_size_in_bytes, expected.total_file_size_in_bytes);
  EXPECT_EQ(r.total_delete_file_size_in_bytes, expected.total_delete_file_size_in_bytes);
  EXPECT_EQ(r.skipped_data_manifests, expected.skipped_data_manifests);
  EXPECT_EQ(r.skipped_delete_manifests, expected.skipped_delete_manifests);
  EXPECT_EQ(r.skipped_data_files, expected.skipped_data_files);
  EXPECT_EQ(r.skipped_delete_files, expected.skipped_delete_files);
  EXPECT_EQ(r.indexed_delete_files, expected.indexed_delete_files);
  EXPECT_EQ(r.equality_delete_files, expected.equality_delete_files);
  EXPECT_EQ(r.positional_delete_files, expected.positional_delete_files);
  EXPECT_EQ(r.dvs, expected.dvs);
}

TEST(ScanMetricsTest, OfContextPopulatesResult) {
  DefaultMetricsContext ctx;
  auto m = ScanMetrics::Of(ctx);
  m.result_data_files->Increment(5);
  m.total_file_size_in_bytes->Increment(1024);
  m.total_planning_duration->Record(std::chrono::nanoseconds{500});

  auto r = m.ToResult();
  EXPECT_EQ(r.result_data_files, 5);
  EXPECT_EQ(r.total_file_size_in_bytes, 1024);
  EXPECT_EQ(r.total_planning_duration.count, 1);
  EXPECT_EQ(r.total_planning_duration.total_duration, std::chrono::nanoseconds{500});
}

// ---------------------------------------------------------------------------
// CommitMetrics
// ---------------------------------------------------------------------------

TEST(CommitMetricsTest, NoopPopulatesZero) {
  auto m = CommitMetrics::Noop();
  CommitMetricsResult result;
  m.PopulateResult(result);
  EXPECT_EQ(result.total_duration.count, 0);
  EXPECT_EQ(result.total_duration.total_duration.count(), 0);
  EXPECT_EQ(result.attempts, 0);
}

TEST(CommitMetricsTest, TimerAndAttemptsPopulated) {
  DefaultMetricsContext ctx;
  auto m = CommitMetrics::Of(ctx);
  m.total_duration->Record(std::chrono::nanoseconds{2000});
  m.attempts->Increment(3);

  CommitMetricsResult result;
  m.PopulateResult(result);
  EXPECT_EQ(result.total_duration.count, 1);
  EXPECT_EQ(result.total_duration.total_duration, std::chrono::nanoseconds{2000});
  EXPECT_EQ(result.attempts, 3);
}

// ---------------------------------------------------------------------------
// JSON serde — CounterResult / TimerResult
// ---------------------------------------------------------------------------

TEST(CounterResultSerdeTest, RoundTrip) {
  CounterResult original{CounterUnit::kBytes, 1024};
  auto json = ToJson(original);
  auto result = CounterResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().unit, CounterUnit::kBytes);
  EXPECT_EQ(result.value().value, 1024);
}

TEST(TimerResultSerdeTest, RoundTrip) {
  TimerResult original{3, std::chrono::nanoseconds{9876}};
  auto json = ToJson(original);
  auto result = TimerResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().count, 3);
  EXPECT_EQ(result.value().total_duration, std::chrono::nanoseconds{9876});
}

// ---------------------------------------------------------------------------
// JSON serde — ScanReport / CommitReport
// ---------------------------------------------------------------------------

TEST(ScanReportSerdeTest, RoundTrip) {
  ScanReport report;
  report.table_name = "cat.db.t";
  report.snapshot_id = 42;
  report.schema_id = 1;
  report.scan_metrics.result_data_files = 7;
  report.scan_metrics.total_file_size_in_bytes = 8192;
  report.scan_metrics.total_planning_duration =
      TimerResult{1, std::chrono::nanoseconds{100000}};
  report.projected_field_ids = {1, 2};
  report.projected_field_names = {"id", "name"};

  auto json_result = ToJson(report);
  ASSERT_TRUE(json_result.has_value());
  auto result = ScanReportFromJson(json_result.value());
  ASSERT_TRUE(result.has_value());
  const auto& r = result.value();
  EXPECT_EQ(r.table_name, "cat.db.t");
  EXPECT_EQ(r.snapshot_id, 42);
  EXPECT_EQ(r.scan_metrics.result_data_files, 7);
  EXPECT_EQ(r.scan_metrics.total_file_size_in_bytes, 8192);
  EXPECT_EQ(r.scan_metrics.total_planning_duration.count, 1);
  EXPECT_EQ(r.scan_metrics.total_planning_duration.total_duration,
            std::chrono::nanoseconds{100000});
  EXPECT_EQ(r.projected_field_ids, (std::vector<int32_t>{1, 2}));
}

TEST(ScanReportSerdeTest, ZeroMetricsOmittedFromJson) {
  ScanReport report;
  report.table_name = "t";
  report.snapshot_id = 1;
  auto json_result = ToJson(report);
  ASSERT_TRUE(json_result.has_value());
  const auto& json = json_result.value();
  EXPECT_TRUE(json.contains("scan-metrics"));
  EXPECT_TRUE(json["scan-metrics"].empty());
}

TEST(ScanReportSerdeTest, RoundTripWithAlwaysTrueFilter) {
  ScanReport report;
  report.table_name = "db.t";
  report.snapshot_id = 1;
  report.filter = True::Instance();

  auto json_result = ToJson(report);
  ASSERT_TRUE(json_result.has_value());
  auto result = ScanReportFromJson(json_result.value());
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value().filter, nullptr);
  EXPECT_EQ(result.value().filter->op(), Expression::Operation::kTrue);
}

TEST(CommitReportSerdeTest, RoundTrip) {
  CommitReport report;
  report.table_name = "cat.db.t";
  report.snapshot_id = 99;
  report.sequence_number = 5;
  report.operation = "append";
  report.commit_metrics.total_duration = TimerResult{1, std::chrono::nanoseconds{200000}};
  report.commit_metrics.attempts = 1;
  report.commit_metrics.added_data_files = 3;
  report.commit_metrics.added_records = 1000;

  auto json = ToJson(report);
  auto result = CommitReportFromJson(json);
  ASSERT_TRUE(result.has_value());
  const auto& r = result.value();
  EXPECT_EQ(r.table_name, "cat.db.t");
  EXPECT_EQ(r.snapshot_id, 99);
  EXPECT_EQ(r.sequence_number, 5);
  EXPECT_EQ(r.operation, "append");
  EXPECT_EQ(r.commit_metrics.total_duration.count, 1);
  EXPECT_EQ(r.commit_metrics.total_duration.total_duration,
            std::chrono::nanoseconds{200000});
  EXPECT_EQ(r.commit_metrics.added_data_files, 3);
  EXPECT_EQ(r.commit_metrics.added_records, 1000);
}

}  // namespace iceberg
