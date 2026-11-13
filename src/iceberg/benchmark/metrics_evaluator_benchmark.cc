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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

constexpr int32_t kNumColumns = 16;
constexpr int64_t kRecordCount = 10'000;
constexpr int64_t kNullCount = 10;

enum class ColumnKind : int32_t { kLong = 0, kInt, kDouble, kString, kNumKinds };

ColumnKind KindOf(int32_t field_id) {
  return static_cast<ColumnKind>((field_id - 1) %
                                 static_cast<int32_t>(ColumnKind::kNumKinds));
}

std::string ColumnName(int32_t field_id) { return "c" + std::to_string(field_id); }

Literal MakeLiteral(int32_t field_id, int64_t number, const char* str) {
  switch (KindOf(field_id)) {
    case ColumnKind::kLong:
      return Literal::Long(number);
    case ColumnKind::kInt:
      return Literal::Int(static_cast<int32_t>(number));
    case ColumnKind::kDouble:
      return Literal::Double(static_cast<double>(number));
    default:
      return Literal::String(str);
  }
}

/// Predicates compare against a value inside the bounds, so every predicate has
/// to deserialize and compare them.
Literal LowerBound(int32_t field_id) { return MakeLiteral(field_id, 100, "abc"); }
Literal UpperBound(int32_t field_id) { return MakeLiteral(field_id, 200, "xyz"); }
Literal ValueInRange(int32_t field_id) { return MakeLiteral(field_id, 150, "mno"); }

std::shared_ptr<Schema> MakeSchema() {
  std::vector<SchemaField> fields;
  fields.reserve(kNumColumns);
  for (int32_t id = 1; id <= kNumColumns; ++id) {
    std::shared_ptr<Type> type;
    switch (KindOf(id)) {
      case ColumnKind::kLong:
        type = int64();
        break;
      case ColumnKind::kInt:
        type = int32();
        break;
      case ColumnKind::kDouble:
        type = float64();
        break;
      default:
        type = string();
        break;
    }
    // Optional, so that IS NULL and NOT NULL are not folded away during binding.
    fields.push_back(SchemaField::MakeOptional(id, ColumnName(id), std::move(type)));
  }
  return std::make_shared<Schema>(std::move(fields), /*schema_id=*/0);
}

DataFile MakeDataFile() {
  DataFile file;
  file.file_path = "s3://bucket/warehouse/db/table/data/00000-0-data.parquet";
  file.file_format = FileFormatType::kParquet;
  file.record_count = kRecordCount;
  file.file_size_in_bytes = int64_t{64} * 1024 * 1024;
  for (int32_t id = 1; id <= kNumColumns; ++id) {
    file.value_counts[id] = kRecordCount;
    file.null_value_counts[id] = kNullCount;
    if (KindOf(id) == ColumnKind::kDouble) {
      file.nan_value_counts[id] = 0;
    }
    file.lower_bounds[id] = LowerBound(id).Serialize().value();
    file.upper_bounds[id] = UpperBound(id).Serialize().value();
  }
  return file;
}

std::shared_ptr<Expression> MakePredicate(int64_t index) {
  const auto id = static_cast<int32_t>(index % kNumColumns) + 1;
  auto name = ColumnName(id);
  switch (index % 6) {
    case 0:
      return Expressions::LessThan(std::move(name), ValueInRange(id));
    case 1:
      return Expressions::GreaterThanOrEqual(std::move(name), ValueInRange(id));
    case 2:
      return Expressions::Equal(std::move(name), ValueInRange(id));
    case 3:
      return Expressions::NotEqual(std::move(name), ValueInRange(id));
    case 4:
      return Expressions::In(std::move(name),
                             {LowerBound(id), ValueInRange(id), UpperBound(id)});
    default:
      return Expressions::NotNull(std::move(name));
  }
}

std::shared_ptr<Expression> MakeFilter(int64_t num_predicates) {
  if (num_predicates <= 0) {
    return Expressions::AlwaysTrue();
  }
  std::shared_ptr<Expression> filter = MakePredicate(0);
  for (int64_t i = 1; i < num_predicates; ++i) {
    filter = Expressions::And(std::move(filter), MakePredicate(i));
  }
  return filter;
}

struct Inclusive {
  static Result<std::unique_ptr<InclusiveMetricsEvaluator>> Make(
      std::shared_ptr<Expression> filter, const std::shared_ptr<Schema>& schema) {
    return InclusiveMetricsEvaluator::Make(std::move(filter), *schema);
  }
};

struct Strict {
  static Result<std::unique_ptr<StrictMetricsEvaluator>> Make(
      std::shared_ptr<Expression> filter, const std::shared_ptr<Schema>& schema) {
    return StrictMetricsEvaluator::Make(std::move(filter), schema);
  }
};

/// Scan planning: ManifestReader reuses one evaluator for every data file.
template <typename EvaluatorT>
void RunEvaluate(benchmark::State& state) {
  const auto schema = MakeSchema();
  const auto file = MakeDataFile();
  auto evaluator = EvaluatorT::Make(MakeFilter(state.range(0)), schema);
  if (!evaluator.has_value()) {
    state.SkipWithError(evaluator.error().message.c_str());
    return;
  }
  // Check once outside the timed loop, so that an error path is never measured.
  if (auto result = (*evaluator)->Evaluate(file); !result.has_value()) {
    state.SkipWithError(result.error().message.c_str());
    return;
  }

  for (auto _ : state) {
    auto result = (*evaluator)->Evaluate(file);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_InclusiveMetricsEvaluate(benchmark::State& state) {
  RunEvaluate<Inclusive>(state);
}

void BM_StrictMetricsEvaluate(benchmark::State& state) { RunEvaluate<Strict>(state); }

void PredicateCounts(benchmark::internal::Benchmark* bench) {
  bench->ArgName("predicates")->RangeMultiplier(4)->Range(1, 64);
}

BENCHMARK(BM_InclusiveMetricsEvaluate)->Apply(PredicateCounts);
BENCHMARK(BM_StrictMetricsEvaluate)->Apply(PredicateCounts);

}  // namespace
}  // namespace iceberg

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
