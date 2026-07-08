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

#include <memory>
#include <vector>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int64()),
          SchemaField::MakeOptional(2, "name", string()),
          SchemaField::MakeRequired(3, "age", int32()),
          SchemaField::MakeOptional(4, "salary", float64()),
          SchemaField::MakeRequired(5, "active", boolean()),
          SchemaField::MakeRequired(6, "date", string()),
      },
      /*schema_id=*/0);
}

std::shared_ptr<DataFile> MakeDataFile() {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "bench_path";
  data_file->file_format = FileFormatType::kParquet;
  data_file->partition.AddValue(Literal::String("20251128"));
  data_file->record_count = 10;
  data_file->file_size_in_bytes = 1024;
  data_file->value_counts = {{1, 10}, {3, 10}, {5, 10}};
  data_file->null_value_counts = {{1, 0}, {3, 0}, {5, 0}};
  data_file->lower_bounds[1] = Literal::Long(100).Serialize().value();
  data_file->upper_bounds[1] = Literal::Long(200).Serialize().value();
  data_file->lower_bounds[3] = Literal::Int(20).Serialize().value();
  data_file->upper_bounds[3] = Literal::Int(40).Serialize().value();
  data_file->lower_bounds[5] = Literal::Boolean(true).Serialize().value();
  data_file->upper_bounds[5] = Literal::Boolean(true).Serialize().value();
  return data_file;
}

std::shared_ptr<Expression> MakeFilter(int predicate_count) {
  std::vector<std::shared_ptr<Expression>> predicates;
  predicates.reserve(predicate_count);
  for (int i = 0; i < predicate_count; ++i) {
    switch (i % 6) {
      case 0:
        predicates.push_back(Expressions::GreaterThan("id", Literal::Long(50)));
        break;
      case 1:
        predicates.push_back(Expressions::LessThan("id", Literal::Long(300)));
        break;
      case 2:
        predicates.push_back(Expressions::GreaterThanOrEqual("age", Literal::Int(18)));
        break;
      case 3:
        predicates.push_back(Expressions::LessThanOrEqual("age", Literal::Int(65)));
        break;
      case 4:
        predicates.push_back(Expressions::Equal("active", Literal::Boolean(true)));
        break;
      default:
        predicates.push_back(Expressions::NotNull("id"));
        break;
    }
  }

  std::shared_ptr<Expression> filter = std::move(predicates.front());
  for (size_t i = 1; i < predicates.size(); ++i) {
    filter = Expressions::And(filter, predicates[i]);
  }
  return filter;
}

struct EvaluatorFixture {
  std::shared_ptr<Schema> schema;
  std::shared_ptr<DataFile> data_file;
  std::unique_ptr<StrictMetricsEvaluator> evaluator;
};

EvaluatorFixture MakeFixture(int predicate_count) {
  EvaluatorFixture fixture;
  fixture.schema = MakeSchema();
  fixture.data_file = MakeDataFile();
  auto filter = MakeFilter(predicate_count);
  auto evaluator = StrictMetricsEvaluator::Make(filter, fixture.schema, true);
  if (!evaluator.has_value()) {
    benchmark::Shutdown();
    std::exit(1);
  }
  fixture.evaluator = std::move(evaluator.value());
  return fixture;
}

void BM_StrictMetricsEvaluate(benchmark::State& state) {
  auto fixture = MakeFixture(static_cast<int>(state.range(0)));
  for (auto _ : state) {
    auto result = fixture.evaluator->Evaluate(*fixture.data_file);
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
}

}  // namespace
}  // namespace iceberg

BENCHMARK(iceberg::BM_StrictMetricsEvaluate)->Arg(1)->Arg(14)->Arg(20);
BENCHMARK_MAIN();
