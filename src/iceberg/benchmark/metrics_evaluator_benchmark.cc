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

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

enum class ExpressionCase {
  kEqualId,
  kRangeId,
  kStartsWithName,
  kSmallInAge,
  kMediumInAge,
  kNullOrNanSalary,
};

template <typename T>
T Must(Result<T> result) {
  if (!result.has_value()) {
    throw std::runtime_error(result.error().message);
  }
  return std::move(result).value();
}

std::shared_ptr<Schema> MakeBenchmarkSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int64()),
          SchemaField::MakeOptional(2, "name", string()),
          SchemaField::MakeRequired(3, "age", int32()),
          SchemaField::MakeOptional(4, "salary", float64()),
          SchemaField::MakeRequired(5, "active", boolean()),
      },
      /*schema_id=*/0);
}

std::vector<Literal> MakeAgeLiterals(int32_t size) {
  std::vector<Literal> values;
  values.reserve(size);
  for (int32_t value = 0; value < size; ++value) {
    values.push_back(Literal::Int(20 + value));
  }
  return values;
}

std::shared_ptr<Expression> MakeExpression(ExpressionCase expression_case) {
  switch (expression_case) {
    case ExpressionCase::kEqualId:
      return Expressions::Equal("id", Literal::Long(150));
    case ExpressionCase::kRangeId:
      return Expressions::And(Expressions::GreaterThanOrEqual("id", Literal::Long(120)),
                              Expressions::LessThan("id", Literal::Long(220)));
    case ExpressionCase::kStartsWithName:
      return Expressions::StartsWith("name", "name-0");
    case ExpressionCase::kSmallInAge:
      return Expressions::In("age", MakeAgeLiterals(4));
    case ExpressionCase::kMediumInAge:
      return Expressions::In("age", MakeAgeLiterals(64));
    case ExpressionCase::kNullOrNanSalary:
      return Expressions::Or(Expressions::IsNull("salary"),
                             Expressions::IsNaN("salary"));
  }
  std::unreachable();
}

std::vector<uint8_t> Serialize(const Literal& literal) {
  return Must(literal.Serialize());
}

DataFile MakeDataFile(int64_t index) {
  auto id_lower = 100 + index % 75;
  auto age_lower = static_cast<int32_t>(20 + index % 40);
  auto name_suffix = std::to_string(1000 + index % 100);
  auto salary_lower = 1.0 + static_cast<double>(index % 25);

  DataFile file;
  file.content = DataFile::Content::kData;
  file.file_path = "file://" + std::to_string(index) + ".parquet";
  file.file_format = FileFormatType::kParquet;
  file.record_count = 1024;
  file.file_size_in_bytes = 64 * 1024;
  file.value_counts = {{1, file.record_count}, {2, file.record_count},
                       {3, file.record_count}, {4, file.record_count},
                       {5, file.record_count}};
  file.null_value_counts = {{1, 0}, {2, 0}, {3, 0}, {4, index % 7 == 0 ? 64 : 0},
                            {5, 0}};
  file.nan_value_counts = {{4, index % 11 == 0 ? 32 : 0}};
  file.lower_bounds = {
      {1, Serialize(Literal::Long(id_lower))},
      {2, Serialize(Literal::String("name-" + name_suffix))},
      {3, Serialize(Literal::Int(age_lower))},
      {4, Serialize(Literal::Double(salary_lower))},
      {5, Serialize(Literal::Boolean(index % 2 == 0))},
  };
  file.upper_bounds = {
      {1, Serialize(Literal::Long(id_lower + 100))},
      {2, Serialize(Literal::String("name-" + std::to_string(1999 + index % 100)))},
      {3, Serialize(Literal::Int(age_lower + 10))},
      {4, Serialize(Literal::Double(salary_lower + 100.0))},
      {5, Serialize(Literal::Boolean(true))},
  };
  file.split_offsets = {4};
  file.sort_order_id = 0;
  return file;
}

std::vector<DataFile> MakeDataFiles(int64_t size) {
  std::vector<DataFile> files;
  files.reserve(size);
  for (int64_t index = 0; index < size; ++index) {
    files.push_back(MakeDataFile(index));
  }
  return files;
}

void BM_InclusiveMetricsEvaluatorMake(benchmark::State& state,
                                      ExpressionCase expression_case) {
  auto schema = MakeBenchmarkSchema();
  auto expression = MakeExpression(expression_case);

  while (state.KeepRunning()) {
    auto evaluator =
        Must(InclusiveMetricsEvaluator::Make(expression, *schema, /*case_sensitive=*/true));
    benchmark::DoNotOptimize(evaluator);
  }

  state.SetItemsProcessed(state.iterations());
}

void BM_StrictMetricsEvaluatorMake(benchmark::State& state,
                                   ExpressionCase expression_case) {
  auto schema = MakeBenchmarkSchema();
  auto expression = MakeExpression(expression_case);

  while (state.KeepRunning()) {
    auto evaluator =
        Must(StrictMetricsEvaluator::Make(expression, schema, /*case_sensitive=*/true));
    benchmark::DoNotOptimize(evaluator);
  }

  state.SetItemsProcessed(state.iterations());
}

void BM_InclusiveMetricsEvaluatorEvaluate(benchmark::State& state,
                                          ExpressionCase expression_case) {
  auto schema = MakeBenchmarkSchema();
  auto expression = MakeExpression(expression_case);
  auto evaluator =
      Must(InclusiveMetricsEvaluator::Make(expression, *schema, /*case_sensitive=*/true));
  auto files = MakeDataFiles(state.range(0));

  while (state.KeepRunning()) {
    int64_t matching_files = 0;
    for (const auto& file : files) {
      matching_files += Must(evaluator->Evaluate(file)) ? 1 : 0;
    }
    benchmark::DoNotOptimize(matching_files);
  }

  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(files.size()));
}

void BM_StrictMetricsEvaluatorEvaluate(benchmark::State& state,
                                       ExpressionCase expression_case) {
  auto schema = MakeBenchmarkSchema();
  auto expression = MakeExpression(expression_case);
  auto evaluator = Must(StrictMetricsEvaluator::Make(expression, schema,
                                                    /*case_sensitive=*/true));
  auto files = MakeDataFiles(state.range(0));

  while (state.KeepRunning()) {
    int64_t matching_files = 0;
    for (const auto& file : files) {
      matching_files += Must(evaluator->Evaluate(file)) ? 1 : 0;
    }
    benchmark::DoNotOptimize(matching_files);
  }

  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(files.size()));
}

void ApplyFileCounts(benchmark::internal::Benchmark* benchmark) {
  benchmark->ArgName("files")->Arg(1)->Arg(100)->Arg(10000);
}

BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, equal_id,
                  ExpressionCase::kEqualId);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, range_id,
                  ExpressionCase::kRangeId);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, starts_with_name,
                  ExpressionCase::kStartsWithName);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, small_in_age,
                  ExpressionCase::kSmallInAge);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, medium_in_age,
                  ExpressionCase::kMediumInAge);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorMake, null_or_nan_salary,
                  ExpressionCase::kNullOrNanSalary);

BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, equal_id, ExpressionCase::kEqualId);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, range_id, ExpressionCase::kRangeId);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, starts_with_name,
                  ExpressionCase::kStartsWithName);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, small_in_age,
                  ExpressionCase::kSmallInAge);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, medium_in_age,
                  ExpressionCase::kMediumInAge);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorMake, null_or_nan_salary,
                  ExpressionCase::kNullOrNanSalary);

BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, equal_id,
                  ExpressionCase::kEqualId)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, range_id,
                  ExpressionCase::kRangeId)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, starts_with_name,
                  ExpressionCase::kStartsWithName)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, small_in_age,
                  ExpressionCase::kSmallInAge)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, medium_in_age,
                  ExpressionCase::kMediumInAge)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_InclusiveMetricsEvaluatorEvaluate, null_or_nan_salary,
                  ExpressionCase::kNullOrNanSalary)
    ->Apply(ApplyFileCounts);

BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, equal_id,
                  ExpressionCase::kEqualId)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, range_id,
                  ExpressionCase::kRangeId)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, starts_with_name,
                  ExpressionCase::kStartsWithName)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, small_in_age,
                  ExpressionCase::kSmallInAge)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, medium_in_age,
                  ExpressionCase::kMediumInAge)
    ->Apply(ApplyFileCounts);
BENCHMARK_CAPTURE(BM_StrictMetricsEvaluatorEvaluate, null_or_nan_salary,
                  ExpressionCase::kNullOrNanSalary)
    ->Apply(ApplyFileCounts);

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
