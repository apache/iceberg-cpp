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
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/json_serde_internal.h"
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
  EXPECT_EQ(c.value(), 0);
  c.Increment();
  EXPECT_EQ(c.value(), 1);
}

TEST(DefaultCounterTest, IncrementByAmount) {
  DefaultCounter c;
  c.Increment(42);
  EXPECT_EQ(c.value(), 42);
  c.Increment(8);
  EXPECT_EQ(c.value(), 50);
}

class DefaultCounterUnitTest : public ::testing::TestWithParam<CounterUnit> {};

TEST_P(DefaultCounterUnitTest, UnitRoundTrips) {
  DefaultCounter c(GetParam());
  EXPECT_EQ(c.unit(), GetParam());
  EXPECT_FALSE(c.IsNoop());
}

INSTANTIATE_TEST_SUITE_P(Units, DefaultCounterUnitTest,
                         ::testing::Values(CounterUnit::kCount, CounterUnit::kBytes),
                         [](const auto& info) {
                           return info.param == CounterUnit::kCount ? "Count" : "Bytes";
                         });

TEST(NoopCounterTest, IsNoopAndAlwaysZero) {
  auto noop = Counter::Noop();
  EXPECT_TRUE(noop->IsNoop());
  noop->Increment();
  noop->Increment(100);
  EXPECT_EQ(noop->value(), 0);
}

// ---------------------------------------------------------------------------
// Timer
// ---------------------------------------------------------------------------

TEST(DefaultTimerTest, RaiiRecordsOnce) {
  DefaultTimer t;
  EXPECT_EQ(t.Count(), 0);
  {
    auto timed = t.Start();
  }
  EXPECT_EQ(t.Count(), 1);  // RAII guard called Record() exactly once
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
  auto noop = Timer::Noop();
  EXPECT_TRUE(noop->IsNoop());
  {
    auto timed = noop->Start();
  }
  EXPECT_EQ(noop->Count(), 0);
  EXPECT_EQ(noop->TotalDuration().count(), 0);
}

TEST(DefaultTimerTest, UnitIsNanoseconds) {
  DefaultTimer t;
  EXPECT_EQ(t.Unit(), "nanoseconds");
  EXPECT_EQ(Timer::Noop()->Unit(), "nanoseconds");
}

struct DurationConversionParam {
  std::string name;
  std::chrono::nanoseconds input;
  std::chrono::nanoseconds expected;
};

class DefaultTimerDurationConversionTest
    : public ::testing::TestWithParam<DurationConversionParam> {};

TEST_P(DefaultTimerDurationConversionTest, RecordsAndConverts) {
  DefaultTimer t;
  t.Record(GetParam().input);
  EXPECT_EQ(t.TotalDuration(), GetParam().expected);
  EXPECT_EQ(t.Count(), 1);
}

INSTANTIATE_TEST_SUITE_P(
    DurationConversion, DefaultTimerDurationConversionTest,
    ::testing::Values(
        DurationConversionParam{"Microseconds",
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::microseconds{5}),
                                std::chrono::nanoseconds{5000}},
        DurationConversionParam{"Milliseconds",
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::milliseconds{2}),
                                std::chrono::nanoseconds{2000000}}),
    [](const auto& info) { return info.param.name; });

TEST(DefaultTimerTest, TimeVoidCallableRecordsOnce) {
  DefaultTimer t;
  t.Time([&] { /* intentional no-op */ });
  // Verify the count was incremented; duration is not checked because a
  // no-op body may measure as 0 ns depending on clock resolution.
  EXPECT_EQ(t.Count(), 1);
}

TEST(DefaultTimerTest, TimeNonVoidCallableReturnsResult) {
  DefaultTimer t;
  int result = t.Time([&] { return 42; });
  EXPECT_EQ(result, 42);
  EXPECT_EQ(t.Count(), 1);
  // Even for Noop callable is still invoked.
  int called = 0;
  Timer::Noop()->Time([&] { ++called; });
  EXPECT_EQ(called, 1);
}

// ---------------------------------------------------------------------------
// MetricsContext
// ---------------------------------------------------------------------------

TEST(DefaultMetricsContextTest, SameNameReturnsSameObject) {
  DefaultMetricsContext ctx;
  auto c1 = ctx.GetCounter("foo", CounterUnit::kCount);
  auto c2 = ctx.GetCounter("foo", CounterUnit::kCount);
  EXPECT_EQ(c1.get(), c2.get());

  auto t1 = ctx.GetTimer("dur");
  auto t2 = ctx.GetTimer("dur");
  EXPECT_EQ(t1.get(), t2.get());
}

TEST(DefaultMetricsContextTest, DifferentNamesReturnDifferentObjects) {
  DefaultMetricsContext ctx;
  auto c1 = ctx.GetCounter("a", CounterUnit::kCount);
  auto c2 = ctx.GetCounter("b", CounterUnit::kCount);
  EXPECT_NE(c1.get(), c2.get());
}

TEST(NullMetricsContextTest, ReturnsNoopInstances) {
  auto null_ctx = MetricsContext::Null();
  EXPECT_TRUE(null_ctx->GetCounter("x", CounterUnit::kCount)->IsNoop());
  EXPECT_TRUE(null_ctx->GetTimer("y")->IsNoop());
}

TEST(NullMetricsContextTest, ReturnsSameSharedPtrEachCall) {
  // Verify the static-shared_ptr fix: no new control block per call.
  auto null_ctx = MetricsContext::Null();
  auto c1 = null_ctx->GetCounter("a", CounterUnit::kCount);
  auto c2 = null_ctx->GetCounter("b", CounterUnit::kCount);
  EXPECT_EQ(c1.get(), c2.get());  // same noop singleton
  auto t1 = null_ctx->GetTimer("x");
  auto t2 = null_ctx->GetTimer("y");
  EXPECT_EQ(t1.get(), t2.get());
}

TEST(DefaultMetricsContextTest, OneArgGetCounterDefaultsToCount) {
  DefaultMetricsContext ctx;
  auto c = ctx.GetCounter("hits");
  EXPECT_NE(c, nullptr);
  EXPECT_EQ(c->unit(), CounterUnit::kCount);
  // Calling again with the same name returns the same object.
  EXPECT_EQ(ctx.GetCounter("hits").get(), c.get());
}

// ---------------------------------------------------------------------------
// ScanMetrics
// ---------------------------------------------------------------------------

TEST(ScanMetricsTest, OfContextPopulatesResult) {
  DefaultMetricsContext ctx;
  auto m = ScanMetrics::Of(ctx);
  m->result_data_files->Increment(5);
  m->total_file_size_in_bytes->Increment(1024);
  m->total_planning_duration->Record(std::chrono::nanoseconds{500});

  auto r = m->ToResult();
  EXPECT_EQ(r.result_data_files.value, 5);
  EXPECT_EQ(r.result_data_files.unit, CounterUnit::kCount);
  EXPECT_EQ(r.total_file_size_in_bytes.value, 1024);
  EXPECT_EQ(r.total_file_size_in_bytes.unit, CounterUnit::kBytes);
  EXPECT_EQ(r.total_planning_duration.count, 1);
  EXPECT_EQ(r.total_planning_duration.total_duration, std::chrono::nanoseconds{500});
}

TEST(ScanMetricsTest, ToResultForwardsTimerUnit) {
  DefaultMetricsContext ctx;
  auto m = ScanMetrics::Of(ctx);
  m->total_planning_duration->Record(std::chrono::nanoseconds{100});
  auto r = m->ToResult();
  EXPECT_EQ(r.total_planning_duration.unit, "nanoseconds");
}

// ---------------------------------------------------------------------------
// CommitMetrics
// ---------------------------------------------------------------------------

TEST(CommitMetricsTest, NoopPopulatesZero) {
  auto m = CommitMetrics::Noop();
  CommitMetricsResult result;
  m->PopulateResult(result);
  EXPECT_EQ(result.total_duration.count, 0);
  EXPECT_EQ(result.total_duration.total_duration.count(), 0);
  EXPECT_EQ(result.attempts.value, 0);
}

TEST(CommitMetricsTest, TimerAndAttemptsPopulated) {
  DefaultMetricsContext ctx;
  auto m = CommitMetrics::Of(ctx);
  m->total_duration->Record(std::chrono::nanoseconds{2000});
  m->attempts->Increment(3);

  CommitMetricsResult result;
  m->PopulateResult(result);
  EXPECT_EQ(result.total_duration.count, 1);
  EXPECT_EQ(result.total_duration.total_duration, std::chrono::nanoseconds{2000});
  EXPECT_EQ(result.attempts.value, 3);
  EXPECT_EQ(result.attempts.unit, CounterUnit::kCount);
}

// ---------------------------------------------------------------------------
// JSON serde — CounterResult / TimerResult
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CounterResult serde — parameterized round-trip
// ---------------------------------------------------------------------------

class CounterResultRoundTripTest : public ::testing::TestWithParam<CounterResult> {};

TEST_P(CounterResultRoundTripTest, RoundTrip) {
  const CounterResult original = GetParam();
  auto json = ToJson(original);
  auto result = CounterResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), original);
}

INSTANTIATE_TEST_SUITE_P(
    CounterResultSerde, CounterResultRoundTripTest,
    ::testing::Values(CounterResult{.unit = CounterUnit::kBytes, .value = 1024},
                      CounterResult{.unit = CounterUnit::kCount, .value = 42}),
    [](const auto& info) {
      return info.param.unit == CounterUnit::kBytes ? "BytesUnit" : "CountUnit";
    });

class TimerResultRoundTripTest : public ::testing::TestWithParam<TimerResult> {};

TEST_P(TimerResultRoundTripTest, RoundTrip) {
  const auto& input = GetParam();
  auto json = ToJson(input);
  EXPECT_EQ(json["unit"], input.unit);
  EXPECT_EQ(json["count"], input.count);
  EXPECT_EQ(json["total-duration"], input.total_duration.count());
  auto result = TimerResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), input);
}

INSTANTIATE_TEST_SUITE_P(
    TimerResultSerde, TimerResultRoundTripTest,
    ::testing::Values(
        TimerResult{.count = 3, .total_duration = std::chrono::nanoseconds{9876}},
        TimerResult{.count = 2, .total_duration = std::chrono::nanoseconds{5000}},
        TimerResult{.unit = "nanoseconds",
                    .count = 1,
                    .total_duration = std::chrono::nanoseconds{999}}),
    [](const auto& info) { return "Count" + std::to_string(info.param.count); });

// ---------------------------------------------------------------------------
// JSON serde — ScanReport / CommitReport
// ---------------------------------------------------------------------------

TEST(ScanReportSerdeTest, RoundTrip) {
  ScanReport report;
  report.table_name = "cat.db.t";
  report.snapshot_id = 42;
  report.schema_id = 1;
  report.scan_metrics.result_data_files = CounterResult{.value = 7};
  report.scan_metrics.total_file_size_in_bytes =
      CounterResult{.unit = CounterUnit::kBytes, .value = 8192};
  report.scan_metrics.total_planning_duration =
      TimerResult{.count = 1, .total_duration = std::chrono::nanoseconds{100000}};
  report.projected_field_ids = {1, 2};
  report.projected_field_names = {"id", "name"};

  auto json_result = ToJson(report);
  ASSERT_TRUE(json_result.has_value());
  auto result = ScanReportFromJson(json_result.value());
  ASSERT_TRUE(result.has_value());
  const auto& r = result.value();
  EXPECT_EQ(r.table_name, "cat.db.t");
  EXPECT_EQ(r.snapshot_id, 42);
  EXPECT_EQ(r.scan_metrics.result_data_files.value, 7);
  EXPECT_EQ(r.scan_metrics.result_data_files.unit, CounterUnit::kCount);
  EXPECT_EQ(r.scan_metrics.total_file_size_in_bytes.value, 8192);
  EXPECT_EQ(r.scan_metrics.total_file_size_in_bytes.unit, CounterUnit::kBytes);
  EXPECT_EQ(r.scan_metrics.total_planning_duration.count, 1);
  EXPECT_EQ(r.scan_metrics.total_planning_duration.total_duration,
            std::chrono::nanoseconds{100000});
  EXPECT_EQ(r.projected_field_ids, (std::vector<int32_t>{1, 2}));
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
  report.commit_metrics.total_duration =
      TimerResult{.count = 1, .total_duration = std::chrono::nanoseconds{200000}};
  report.commit_metrics.attempts = CounterResult{.value = 1};
  report.commit_metrics.added_data_files = CounterResult{.value = 3};
  report.commit_metrics.added_records = CounterResult{.value = 1000};

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
  EXPECT_EQ(r.commit_metrics.added_data_files.value, 3);
  EXPECT_EQ(r.commit_metrics.added_records.value, 1000);
}

// ---------------------------------------------------------------------------
// ScanMetricsResult::From
// ---------------------------------------------------------------------------

TEST(ScanMetricsResultTest, FromDelegatesToToResult) {
  DefaultMetricsContext ctx;
  auto m = ScanMetrics::Of(ctx);
  m->result_data_files->Increment(7);
  m->total_planning_duration->Record(std::chrono::nanoseconds{12345});

  auto via_from = ScanMetricsResult::From(*m);
  auto via_to_result = m->ToResult();

  EXPECT_EQ(via_from.result_data_files, via_to_result.result_data_files);
  EXPECT_EQ(via_from.total_planning_duration.count,
            via_to_result.total_planning_duration.count);
  EXPECT_EQ(via_from.total_planning_duration.total_duration,
            via_to_result.total_planning_duration.total_duration);
}

// ---------------------------------------------------------------------------
// CommitMetricsResult::From
// ---------------------------------------------------------------------------

TEST(CommitMetricsResultTest, FromWithEmptySummaryYieldsZeroFileCounts) {
  DefaultMetricsContext ctx;
  auto live = CommitMetrics::Of(ctx);
  live->total_duration->Record(std::chrono::nanoseconds{5000});
  live->attempts->Increment();

  auto result = CommitMetricsResult::From(*live, {});

  EXPECT_EQ(result.total_duration.count, 1);
  EXPECT_EQ(result.total_duration.total_duration, std::chrono::nanoseconds{5000});
  EXPECT_EQ(result.attempts.value, 1);
  EXPECT_EQ(result.attempts.unit, CounterUnit::kCount);
  // All snapshot-summary fields must be zero when the summary is empty.
  EXPECT_EQ(result.added_data_files.value, 0);
  EXPECT_EQ(result.added_data_files.unit, CounterUnit::kCount);
  EXPECT_EQ(result.removed_data_files.value, 0);
  EXPECT_EQ(result.total_data_files.value, 0);
  EXPECT_EQ(result.added_records.value, 0);
  EXPECT_EQ(result.total_records.value, 0);
  EXPECT_EQ(result.kept_manifest_count.value, 0);
  EXPECT_EQ(result.created_manifest_count.value, 0);
}

TEST(CommitMetricsResultTest, FromParsesSnapshotSummary) {
  DefaultMetricsContext ctx;
  auto live = CommitMetrics::Of(ctx);
  live->total_duration->Record(std::chrono::nanoseconds{8000});
  live->attempts->Increment(2);

  std::unordered_map<std::string, std::string> summary = {
      {"added-data-files", "3"},     {"deleted-data-files", "1"},
      {"total-data-files", "10"},    {"added-records", "1000"},
      {"deleted-records", "200"},    {"total-records", "5000"},
      {"added-files-size", "4096"},  {"removed-files-size", "1024"},
      {"total-files-size", "20480"}, {"manifests-created", "2"},
      {"manifests-kept", "5"},       {"manifests-replaced", "1"},
      {"entries-processed", "8"},
  };

  auto result = CommitMetricsResult::From(*live, summary);

  // Live metrics.
  EXPECT_EQ(result.total_duration.count, 1);
  EXPECT_EQ(result.total_duration.total_duration, std::chrono::nanoseconds{8000});
  EXPECT_EQ(result.attempts.value, 2);
  EXPECT_EQ(result.attempts.unit, CounterUnit::kCount);

  // Snapshot-summary fields — verify both value and unit.
  EXPECT_EQ(result.added_data_files.value, 3);
  EXPECT_EQ(result.added_data_files.unit, CounterUnit::kCount);
  EXPECT_EQ(result.removed_data_files.value, 1);
  EXPECT_EQ(result.total_data_files.value, 10);
  EXPECT_EQ(result.added_records.value, 1000);
  EXPECT_EQ(result.removed_records.value, 200);
  EXPECT_EQ(result.total_records.value, 5000);
  EXPECT_EQ(result.added_files_size_bytes.value, 4096);
  EXPECT_EQ(result.added_files_size_bytes.unit, CounterUnit::kBytes);
  EXPECT_EQ(result.removed_files_size_bytes.value, 1024);
  EXPECT_EQ(result.removed_files_size_bytes.unit, CounterUnit::kBytes);
  EXPECT_EQ(result.total_files_size_bytes.value, 20480);
  EXPECT_EQ(result.total_files_size_bytes.unit, CounterUnit::kBytes);
  EXPECT_EQ(result.created_manifest_count.value, 2);
  EXPECT_EQ(result.kept_manifest_count.value, 5);
  EXPECT_EQ(result.replaced_manifest_count.value, 1);
  EXPECT_EQ(result.processed_manifest_entries_count.value, 8);
}

TEST(CommitMetricsResultTest, FromHandlesMissingAndUnparseableKeys) {
  // Missing key → 0, unparseable value → 0.
  std::unordered_map<std::string, std::string> summary = {
      {"added-data-files", "not-a-number"},
      // "deleted-data-files" intentionally absent
  };
  auto result = CommitMetricsResult::From(*CommitMetrics::Noop(), summary);
  EXPECT_EQ(result.added_data_files.value, 0);    // unparseable
  EXPECT_EQ(result.removed_data_files.value, 0);  // absent
}

// ---------------------------------------------------------------------------
// Metrics JSON serde — CounterResult (additional cases)
// ---------------------------------------------------------------------------

TEST(CounterResultSerdeTest, MissingUnitDefaultsToCount) {
  nlohmann::json json;
  json["value"] = 7;
  // No "unit" key — should default to kCount.
  auto result = CounterResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().unit, CounterUnit::kCount);
  EXPECT_EQ(result.value().value, 7);
}

TEST(CounterResultSerdeTest, MissingValueReturnsError) {
  nlohmann::json json;
  json["unit"] = "count";
  // Missing "value" key — must return an error.
  auto result = CounterResultFromJson(json);
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// Metrics JSON serde — ScanMetricsResult
// ---------------------------------------------------------------------------

TEST(ScanMetricsResultSerdeTest, AllFieldsRoundTrip) {
  ScanMetricsResult m;
  m.total_planning_duration =
      TimerResult{.count = 2, .total_duration = std::chrono::nanoseconds{50000}};
  m.result_data_files = CounterResult{.unit = CounterUnit::kCount, .value = 10};
  m.result_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.scanned_data_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 5};
  m.scanned_delete_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.total_data_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 8};
  m.total_delete_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 3};
  m.total_file_size_in_bytes =
      CounterResult{.unit = CounterUnit::kBytes, .value = 131072};
  m.total_delete_file_size_in_bytes =
      CounterResult{.unit = CounterUnit::kBytes, .value = 4096};
  m.skipped_data_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 3};
  m.skipped_delete_manifests = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.skipped_data_files = CounterResult{.unit = CounterUnit::kCount, .value = 7};
  m.skipped_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.indexed_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 4};
  m.equality_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.positional_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.dvs = CounterResult{.unit = CounterUnit::kCount, .value = 3};

  auto json = ToJson(m);
  auto result = ScanMetricsResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), m);
}

TEST(ScanMetricsResultSerdeTest, MissingFieldsDefaultToZeroCounterResult) {
  // JSON with only one field set; all others must default to CounterResult{}.
  nlohmann::json json = nlohmann::json::object();
  json["result-data-files"] = nlohmann::json{{"unit", "count"}, {"value", 5}};

  auto result = ScanMetricsResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().result_data_files.value, 5);
  EXPECT_EQ(result.value().result_delete_files, CounterResult{});
  EXPECT_EQ(result.value().total_file_size_in_bytes, CounterResult{});
}

TEST(ScanMetricsResultSerdeTest, JsonKeyNamesAreKebabCase) {
  ScanMetricsResult m;
  m.result_data_files = CounterResult{.value = 1};
  m.total_file_size_in_bytes = CounterResult{.unit = CounterUnit::kBytes, .value = 1};
  m.total_planning_duration =
      TimerResult{.count = 1, .total_duration = std::chrono::nanoseconds{1}};

  auto json = ToJson(m);
  EXPECT_TRUE(json.contains("result-data-files"));
  EXPECT_TRUE(json.contains("total-file-size-in-bytes"));
  EXPECT_TRUE(json.contains("total-planning-duration"));
  // Spot-check that no camelCase or snake_case keys leaked in.
  EXPECT_FALSE(json.contains("resultDataFiles"));
  EXPECT_FALSE(json.contains("result_data_files"));
}

// ---------------------------------------------------------------------------
// Metrics JSON serde — CommitMetricsResult
// ---------------------------------------------------------------------------

TEST(CommitMetricsResultSerdeTest, EmptyResultProducesEmptyJsonObject) {
  CommitMetricsResult empty{};
  auto json = ToJson(empty);
  EXPECT_TRUE(json.is_object());
  EXPECT_TRUE(json.empty()) << "all-zero CommitMetricsResult must produce empty JSON";
}

TEST(CommitMetricsResultSerdeTest, AllFieldsRoundTrip) {
  CommitMetricsResult m;
  m.total_duration =
      TimerResult{.count = 1, .total_duration = std::chrono::nanoseconds{1000}};
  m.attempts = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.added_data_files = CounterResult{.unit = CounterUnit::kCount, .value = 3};
  m.removed_data_files = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.total_data_files = CounterResult{.unit = CounterUnit::kCount, .value = 10};
  m.added_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.added_equality_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.added_positional_delete_files =
      CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.added_dvs = CounterResult{.unit = CounterUnit::kCount, .value = 4};
  m.removed_positional_delete_files =
      CounterResult{.unit = CounterUnit::kCount, .value = 0};
  m.removed_dvs = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.removed_equality_delete_files =
      CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.removed_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.total_delete_files = CounterResult{.unit = CounterUnit::kCount, .value = 5};
  m.added_records = CounterResult{.unit = CounterUnit::kCount, .value = 500};
  m.removed_records = CounterResult{.unit = CounterUnit::kCount, .value = 100};
  m.total_records = CounterResult{.unit = CounterUnit::kCount, .value = 2000};
  m.added_files_size_bytes = CounterResult{.unit = CounterUnit::kBytes, .value = 8192};
  m.removed_files_size_bytes = CounterResult{.unit = CounterUnit::kBytes, .value = 1024};
  m.total_files_size_bytes = CounterResult{.unit = CounterUnit::kBytes, .value = 65536};
  m.added_positional_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 20};
  m.removed_positional_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 5};
  m.total_positional_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 50};
  m.added_equality_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 10};
  m.removed_equality_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 3};
  m.total_equality_deletes = CounterResult{.unit = CounterUnit::kCount, .value = 30};
  m.kept_manifest_count = CounterResult{.unit = CounterUnit::kCount, .value = 4};
  m.created_manifest_count = CounterResult{.unit = CounterUnit::kCount, .value = 2};
  m.replaced_manifest_count = CounterResult{.unit = CounterUnit::kCount, .value = 1};
  m.processed_manifest_entries_count =
      CounterResult{.unit = CounterUnit::kCount, .value = 12};

  auto json = ToJson(m);
  auto result = CommitMetricsResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), m);
}

TEST(CommitMetricsResultSerdeTest, ZeroValueFieldsOmittedFromJson) {
  CommitMetricsResult m;
  m.added_data_files = CounterResult{.value = 5};
  // All other fields remain zero.
  auto json = ToJson(m);
  EXPECT_TRUE(json.contains("added-data-files"));
  EXPECT_FALSE(json.contains("removed-data-files"));
  EXPECT_FALSE(json.contains("total-duration"));
  EXPECT_FALSE(json.contains("attempts"));
}

TEST(CommitMetricsResultSerdeTest, MissingFieldsDefaultToZeroCounterResult) {
  nlohmann::json json = nlohmann::json::object();
  json["added-data-files"] = nlohmann::json{{"unit", "count"}, {"value", 9}};

  auto result = CommitMetricsResultFromJson(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().added_data_files.value, 9);
  EXPECT_EQ(result.value().removed_data_files, CounterResult{});
  EXPECT_EQ(result.value().total_duration, TimerResult{});
}

// ---------------------------------------------------------------------------
// Metrics JSON serde — CommitReport (additional cases)
// ---------------------------------------------------------------------------

TEST(CommitReportSerdeTest, ZeroMetricsOmittedFromJson) {
  CommitReport report;
  report.table_name = "db.t";
  report.snapshot_id = 1;
  report.sequence_number = 1;
  auto json = ToJson(report);
  EXPECT_TRUE(json.contains("commit-metrics"));
  EXPECT_TRUE(json["commit-metrics"].empty());
}

struct ReportRequiredFieldParam {
  std::string name;
  nlohmann::json json;
  std::function<bool(const nlohmann::json&)> has_value;
};

class ReportRequiredFieldTest
    : public ::testing::TestWithParam<ReportRequiredFieldParam> {};

TEST_P(ReportRequiredFieldTest, MissingRequiredFieldReturnsError) {
  EXPECT_FALSE(GetParam().has_value(GetParam().json));
}

INSTANTIATE_TEST_SUITE_P(
    RequiredFields, ReportRequiredFieldTest,
    ::testing::Values(
        ReportRequiredFieldParam{
            "ScanMissingSnapshotId", nlohmann::json{{"table-name", "t"}},
            [](const nlohmann::json& j) { return ScanReportFromJson(j).has_value(); }},
        ReportRequiredFieldParam{
            "CommitMissingTableName",
            nlohmann::json{{"snapshot-id", 1}, {"sequence-number", 1}},
            [](const nlohmann::json& j) { return CommitReportFromJson(j).has_value(); }}),
    [](const auto& info) { return info.param.name; });

}  // namespace iceberg
