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

// Compares serial and parallel scans of local tables.
//
// Two workloads are written under --dir: a few large multi-row-group Parquet files,
// and hundreds of small Parquet files. For each workload the benchmark runs paired
// trials of the serial path (PlanFilesStream() and FileScanTaskReader) and the
// parallel path (PlanTasksStream() and ParallelScanReader) with the same projection,
// filter, and batch size, alternating which runs first. It reports the bootstrapped
// median and 95% confidence interval of the paired elapsed-time speedup, end to end
// and for the read phase. Timings are not stable enough for CI assertions.
//
// Usage: parallel_scan_benchmark [--dir=PATH] [--trials=N] [--workers=N]
//                                [--workload=large|small|all] [--sweep]
//                                [--filter=all-pass|none]

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <sys/resource.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/parallel_scan_reader.h"
#include "iceberg/data/parallel_scan_reader_internal.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

using Clock = std::chrono::steady_clock;

struct Workload {
  std::string name;
  int files;
  int64_t rows_per_file;
  int64_t rows_per_row_group;
  // read.split.target-size table property, so large files are split into ranges.
  int64_t target_split_size;
};

struct Config {
  std::filesystem::path dir = std::filesystem::temp_directory_path() / "iceberg-pss";
  int trials = 10;
  int primary_workers = 0;
  std::string workload = "all";
  bool sweep = false;
  // Whether scans carry an always-true residual filter that is evaluated per row.
  bool filter = true;
};

// One timed scan.
// Process-wide I/O and Arrow default-pool counters. A scan's cost is the
// difference between readings taken before and after it.
struct Counters {
  // Bytes read through read(2)-family calls (/proc/self/io rchar); -1 if unavailable.
  int64_t bytes_read = -1;
  int64_t allocations = 0;
  int64_t allocated_bytes = 0;

  Counters operator-(const Counters& before) const {
    return {.bytes_read = bytes_read < 0 || before.bytes_read < 0
                              ? -1
                              : bytes_read - before.bytes_read,
            .allocations = allocations - before.allocations,
            .allocated_bytes = allocated_bytes - before.allocated_bytes};
  }
};

Counters ReadCounters() {
  Counters counters;
  std::ifstream io("/proc/self/io");
  std::string key;
  int64_t value = 0;
  while (io >> key >> value) {
    if (key == "rchar:") {
      counters.bytes_read = value;
      break;
    }
  }
  auto* pool = ::arrow::default_memory_pool();
  counters.allocations = pool->num_allocations();
  counters.allocated_bytes = pool->total_bytes_allocated();
  return counters;
}

struct Run {
  Counters counters;
  double total_seconds = 0;
  double read_seconds = 0;
  int64_t rows = 0;
  int64_t batches = 0;
  std::optional<internal::ParallelScanMetricsSnapshot> metrics;
};

std::shared_ptr<Schema> TableSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                               SchemaField::MakeRequired(2, "data", string()),
                               SchemaField::MakeRequired(3, "value", float64())});
}

Status Check(const ::arrow::Status& status) {
  if (!status.ok()) {
    return IOError("{}", status.ToString());
  }
  return {};
}

Result<std::shared_ptr<DataFile>> WriteFile(const std::shared_ptr<FileIO>& io,
                                            const std::shared_ptr<Schema>& schema,
                                            const std::string& path, int64_t first_id,
                                            const Workload& workload) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      DataWriter::Make(
          {.path = path,
           .schema = schema,
           .spec = PartitionSpec::Unpartitioned(),
           .partition = PartitionValues{},
           .format = FileFormatType::kParquet,
           .io = io,
           .properties = {{"write.parquet.max-row-group-rows",
                           std::to_string(workload.rows_per_row_group)},
                          {"write.parquet.compression-codec", "uncompressed"}}}));
  constexpr int64_t kChunkRows = 65536;
  for (int64_t start = 0; start < workload.rows_per_file; start += kChunkRows) {
    const int64_t rows = std::min(kChunkRows, workload.rows_per_file - start);
    ::arrow::Int64Builder ids;
    ::arrow::StringBuilder data;
    ::arrow::DoubleBuilder values;
    for (int64_t row = 0; row < rows; ++row) {
      const int64_t id = first_id + start + row;
      ICEBERG_RETURN_UNEXPECTED(Check(ids.Append(id)));
      ICEBERG_RETURN_UNEXPECTED(
          Check(data.Append(std::format("payload-{:08}-{}", id % 100000, id % 7))));
      ICEBERG_RETURN_UNEXPECTED(Check(values.Append(static_cast<double>(id) * 0.5)));
    }
    std::shared_ptr<::arrow::Array> id_array;
    std::shared_ptr<::arrow::Array> data_array;
    std::shared_ptr<::arrow::Array> value_array;
    ICEBERG_RETURN_UNEXPECTED(Check(ids.Finish(&id_array)));
    ICEBERG_RETURN_UNEXPECTED(Check(data.Finish(&data_array)));
    ICEBERG_RETURN_UNEXPECTED(Check(values.Finish(&value_array)));
    auto batch =
        ::arrow::StructArray::Make({id_array, data_array, value_array},
                                   std::vector<std::string>{"id", "data", "value"});
    if (!batch.ok()) {
      return IOError("{}", batch.status().ToString());
    }
    ArrowArray c_array{};
    ICEBERG_RETURN_UNEXPECTED(Check(::arrow::ExportArray(**batch, &c_array)));
    auto status = writer->Write(&c_array);
    if (c_array.release != nullptr) {
      c_array.release(&c_array);
    }
    ICEBERG_RETURN_UNEXPECTED(status);
  }
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
  return metadata.data_files.at(0);
}

Result<std::shared_ptr<Table>> CreateTable(const Config& config,
                                           const std::shared_ptr<FileIO>& io,
                                           const Workload& workload) {
  const auto table_dir = config.dir / workload.name;
  std::filesystem::remove_all(table_dir);
  std::filesystem::create_directories(table_dir / "data");
  std::filesystem::create_directories(table_dir / "metadata");

  ICEBERG_ASSIGN_OR_RAISE(auto catalog,
                          InMemoryCatalog::Make("bench", io, config.dir.string(), {}));
  const Namespace ns{.levels = {"bench"}};
  ICEBERG_RETURN_UNEXPECTED(catalog->CreateNamespace(ns, {}));
  const TableIdentifier identifier{.ns = ns, .name = workload.name};
  auto schema = TableSchema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto table, catalog->CreateTable(identifier, schema, PartitionSpec::Unpartitioned(),
                                       SortOrder::Unsorted(), table_dir.string(),
                                       {{"format-version", "2"},
                                        {"read.split.target-size",
                                         std::to_string(workload.target_split_size)}}));

  ICEBERG_ASSIGN_OR_RAISE(auto append, table->NewFastAppend());
  for (int file = 0; file < workload.files; ++file) {
    const auto path = (table_dir / "data" / std::format("{:05}.parquet", file)).string();
    ICEBERG_ASSIGN_OR_RAISE(
        auto data_file,
        WriteFile(io, schema, path, file * workload.rows_per_file, workload));
    append->AppendFile(data_file);
  }
  ICEBERG_RETURN_UNEXPECTED(append->Commit());
  return catalog->LoadTable(identifier);
}

// Every row passes, so the residual is evaluated without changing the result size.
std::shared_ptr<Expression> ScanFilter() {
  return Expressions::GreaterThanOrEqual("id", Literal::Long(0));
}

FileScanTaskReader::Options ReaderOptions(const std::shared_ptr<FileIO>& io,
                                          const std::shared_ptr<Schema>& schema) {
  return {.io = io,
          .table_schema = schema,
          .projected_schema = schema,
          .case_sensitive = true,
          .properties = {{"read.batch-size", "8192"}},
          .apply_residuals = true};
}

Result<std::unique_ptr<DataTableScan>> BuildScan(const Table& table, bool filter) {
  ICEBERG_ASSIGN_OR_RAISE(auto builder, table.NewScan());
  if (filter) {
    builder->Filter(ScanFilter());
  }
  return builder->Build();
}

Result<int64_t> Drain(ArrowArrayStream* stream, int64_t* batches) {
  int64_t rows = 0;
  while (true) {
    ArrowArray array{};
    if (int code = stream->get_next(stream, &array); code != 0) {
      const char* message = stream->get_last_error(stream);
      return IOError("get_next failed ({}): {}", code, message ? message : "");
    }
    if (array.release == nullptr) {
      return rows;
    }
    rows += array.length;
    ++*batches;
    array.release(&array);
  }
}

double Seconds(Clock::duration duration) {
  return std::chrono::duration<double>(duration).count();
}

Result<Run> RunSerial(const Table& table, const std::shared_ptr<FileIO>& io,
                      const std::shared_ptr<Schema>& schema, bool filter) {
  Run run;
  const auto before = ReadCounters();
  const auto start = Clock::now();
  ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(table, filter));
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFilesStream());
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          FileScanTaskReader::Make(ReaderOptions(io, schema)));
  std::optional<Clock::time_point> read_start;
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto task, tasks->Next());
    if (!task.has_value()) {
      break;
    }
    if (!read_start.has_value()) {
      read_start = Clock::now();
    }
    ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(**task));
    auto rows = Drain(&stream, &run.batches);
    stream.release(&stream);
    ICEBERG_ASSIGN_OR_RAISE(auto count, std::move(rows));
    run.rows += count;
  }
  tasks.reset();
  const auto end = Clock::now();
  run.counters = ReadCounters() - before;
  run.total_seconds = Seconds(end - start);
  run.read_seconds = Seconds(end - read_start.value_or(end));
  return run;
}

Result<Run> RunParallel(const Table& table, const std::shared_ptr<FileIO>& io,
                        const std::shared_ptr<Schema>& schema, int workers, bool filter) {
  Run run;
  auto metrics = std::make_shared<internal::ParallelScanMetrics>();
  const auto before = ReadCounters();
  const auto start = Clock::now();
  ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(table, filter));
  ICEBERG_ASSIGN_OR_RAISE(auto groups, scan->PlanTasksStream());
  ICEBERG_ASSIGN_OR_RAISE(auto stream, internal::OpenParallelScan(
                                           std::move(groups),
                                           {.reader_options = ReaderOptions(io, schema),
                                            .worker_count = workers,
                                            .max_queued_batches = 2 * workers,
                                            .max_queued_bytes = int64_t{64} << 20},
                                           metrics));
  // Workers claim the first group as soon as the stream opens.
  const auto read_start = Clock::now();
  auto rows = Drain(&stream, &run.batches);
  stream.release(&stream);
  const auto end = Clock::now();
  run.counters = ReadCounters() - before;
  ICEBERG_ASSIGN_OR_RAISE(run.rows, std::move(rows));
  run.total_seconds = Seconds(end - start);
  run.read_seconds = Seconds(end - read_start);
  run.metrics = metrics->Snapshot();
  return run;
}

Result<double> PlanningSeconds(const Table& table, bool filter) {
  const auto start = Clock::now();
  ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(table, filter));
  ICEBERG_ASSIGN_OR_RAISE(auto groups, scan->PlanTasks());
  return Seconds(Clock::now() - start);
}

struct Interval {
  double median = 0;
  double low = 0;
  double high = 0;
};

double Median(std::vector<double> values) {
  std::ranges::sort(values);
  const size_t mid = values.size() / 2;
  return values.size() % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
}

// Percentile bootstrap of the median of paired ratios.
Interval BootstrapMedian(const std::vector<double>& ratios) {
  constexpr int kResamples = 10000;
  std::mt19937_64 random(20261123);
  std::uniform_int_distribution<size_t> pick(0, ratios.size() - 1);
  std::vector<double> medians;
  medians.reserve(kResamples);
  std::vector<double> sample(ratios.size());
  for (int resample = 0; resample < kResamples; ++resample) {
    for (auto& value : sample) {
      value = ratios[pick(random)];
    }
    medians.push_back(Median(sample));
  }
  std::ranges::sort(medians);
  return Interval{.median = Median(ratios),
                  .low = medians[static_cast<size_t>(0.025 * kResamples)],
                  .high = medians[static_cast<size_t>(0.975 * kResamples) - 1]};
}

double CoefficientOfVariation(const std::vector<double>& values) {
  const double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
  double sum = 0;
  for (double value : values) {
    sum += (value - mean) * (value - mean);
  }
  return std::sqrt(sum / (values.size() - 1)) / mean;
}

int64_t PeakRssKiB() {
  rusage usage{};
  getrusage(RUSAGE_SELF, &usage);
  return usage.ru_maxrss;
}

Status RunWorkload(const Config& config, const Workload& workload) {
  auto io = std::shared_ptr<FileIO>(arrow::ArrowFileSystemFileIO::MakeLocalFileIO());
  const auto setup_start = Clock::now();
  ICEBERG_ASSIGN_OR_RAISE(auto table, CreateTable(config, io, workload));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table->schema());
  ICEBERG_ASSIGN_OR_RAISE(auto plan, BuildScan(*table, config.filter));
  ICEBERG_ASSIGN_OR_RAISE(auto groups, plan->PlanTasks());
  size_t tasks = 0;
  for (const auto& group : groups) {
    tasks += group->tasks().size();
  }
  std::cout << std::format(
      "\n== Workload {}: {} files x {} rows, {} rows per row group; {} groups, {} tasks "
      "(setup {:.1f}s)\n",
      workload.name, workload.files, workload.rows_per_file, workload.rows_per_row_group,
      groups.size(), tasks, Seconds(Clock::now() - setup_start));

  // Warm the page cache once; runs are labeled warm.
  ICEBERG_ASSIGN_OR_RAISE(auto warm, RunSerial(*table, io, schema, config.filter));
  const int64_t expected_rows = workload.files * workload.rows_per_file;
  if (warm.rows != expected_rows) {
    return Invalid("Serial scan read {} rows, expected {}", warm.rows, expected_rows);
  }

  std::vector<double> planning;
  for (int trial = 0; trial < 5; ++trial) {
    ICEBERG_ASSIGN_OR_RAISE(auto seconds, PlanningSeconds(*table, config.filter));
    planning.push_back(seconds);
  }
  std::cout << std::format("planning (PlanTasks, median of 5): {:.4f}s\n",
                           Median(planning));

  if (config.sweep) {
    ICEBERG_ASSIGN_OR_RAISE(auto serial, RunSerial(*table, io, schema, config.filter));
    std::cout << std::format("sweep serial: {:.3f}s, {:.2f} Mrows/s\n",
                             serial.total_seconds,
                             serial.rows / serial.total_seconds / 1e6);
    for (int workers : {1, 2, 4, 8}) {
      ICEBERG_ASSIGN_OR_RAISE(auto run,
                              RunParallel(*table, io, schema, workers, config.filter));
      if (run.rows != expected_rows) {
        return Invalid("Parallel scan read {} rows, expected {}", run.rows,
                       expected_rows);
      }
      std::cout << std::format("sweep {} workers: {:.3f}s, {:.2f} Mrows/s\n", workers,
                               run.total_seconds, run.rows / run.total_seconds / 1e6);
    }
  }

  std::vector<double> serial_total;
  std::vector<double> parallel_total;
  std::vector<double> total_ratios;
  std::vector<double> read_ratios;
  // Indexed by path: 0 = serial, 1 = parallel.
  std::array<std::vector<double>, 2> bytes_read;
  std::array<std::vector<double>, 2> allocations;
  std::array<std::vector<double>, 2> allocated_bytes;
  internal::ParallelScanMetricsSnapshot peak;
  for (int trial = 0; trial < config.trials; ++trial) {
    Run serial;
    Run parallel;
    if (trial % 2 == 0) {
      ICEBERG_ASSIGN_OR_RAISE(serial, RunSerial(*table, io, schema, config.filter));
      ICEBERG_ASSIGN_OR_RAISE(
          parallel,
          RunParallel(*table, io, schema, config.primary_workers, config.filter));
    } else {
      ICEBERG_ASSIGN_OR_RAISE(
          parallel,
          RunParallel(*table, io, schema, config.primary_workers, config.filter));
      ICEBERG_ASSIGN_OR_RAISE(serial, RunSerial(*table, io, schema, config.filter));
    }
    if (serial.rows != expected_rows || parallel.rows != expected_rows) {
      return Invalid("Row count mismatch: serial {}, parallel {}, expected {}",
                     serial.rows, parallel.rows, expected_rows);
    }
    serial_total.push_back(serial.total_seconds);
    parallel_total.push_back(parallel.total_seconds);
    total_ratios.push_back(serial.total_seconds / parallel.total_seconds);
    read_ratios.push_back(serial.read_seconds / parallel.read_seconds);
    for (const auto& [path, run] : {std::pair{0, &serial}, std::pair{1, &parallel}}) {
      bytes_read[path].push_back(static_cast<double>(run->counters.bytes_read));
      allocations[path].push_back(static_cast<double>(run->counters.allocations));
      allocated_bytes[path].push_back(static_cast<double>(run->counters.allocated_bytes));
    }
    const auto& metrics = *parallel.metrics;
    peak.peak_queued_bytes = std::max(peak.peak_queued_bytes, metrics.peak_queued_bytes);
    peak.peak_queued_batches =
        std::max(peak.peak_queued_batches, metrics.peak_queued_batches);
    peak.peak_held_bytes = std::max(peak.peak_held_bytes, metrics.peak_held_bytes);
    peak.oversized_admissions += metrics.oversized_admissions;
    peak.producer_wait += metrics.producer_wait;
    peak.tasks_opened = metrics.tasks_opened;
  }

  const auto total = BootstrapMedian(total_ratios);
  const auto read = BootstrapMedian(read_ratios);
  std::cout << std::format(
      "paired trials: {} (serial vs {} workers, alternating order)\n"
      "serial   median {:.3f}s (CV {:.1f}%), {:.2f} Mrows/s\n"
      "parallel median {:.3f}s (CV {:.1f}%), {:.2f} Mrows/s\n"
      "end-to-end speedup: median {:.2f}x, 95% CI [{:.2f}, {:.2f}]\n"
      "read-phase speedup: median {:.2f}x, 95% CI [{:.2f}, {:.2f}]\n"
      "parallel queue: peak {} bytes / {} batches, peak held {} bytes, "
      "{} oversized admissions, producer wait {:.3f}s total, {} task opens per scan\n"
      "per scan (median, serial / parallel): bytes read {:.1f} / {:.1f} MiB, "
      "Arrow pool allocations {:.0f} / {:.0f} ({:.1f} / {:.1f} MiB)\n"
      "peak process RSS so far: {} MiB\n",
      config.trials, config.primary_workers, Median(serial_total),
      100 * CoefficientOfVariation(serial_total),
      expected_rows / Median(serial_total) / 1e6, Median(parallel_total),
      100 * CoefficientOfVariation(parallel_total),
      expected_rows / Median(parallel_total) / 1e6, total.median, total.low, total.high,
      read.median, read.low, read.high, peak.peak_queued_bytes, peak.peak_queued_batches,
      peak.peak_held_bytes, peak.oversized_admissions,
      std::chrono::duration<double>(peak.producer_wait).count(), peak.tasks_opened,
      Median(bytes_read[0]) / (1 << 20), Median(bytes_read[1]) / (1 << 20),
      Median(allocations[0]), Median(allocations[1]),
      Median(allocated_bytes[0]) / (1 << 20), Median(allocated_bytes[1]) / (1 << 20),
      PeakRssKiB() / 1024);
  return {};
}

int PrimaryWorkers() {
  const unsigned cores = std::max(1U, std::thread::hardware_concurrency());
  if (cores >= 16) {
    return 8;
  }
  return cores >= 8 ? 4 : 2;
}

Config ParseArgs(int argc, char** argv) {
  Config config;
  for (int index = 1; index < argc; ++index) {
    std::string_view arg = argv[index];
    auto value = [&](std::string_view prefix) -> std::optional<std::string> {
      if (arg.starts_with(prefix)) {
        return std::string(arg.substr(prefix.size()));
      }
      return std::nullopt;
    };
    if (auto dir = value("--dir=")) {
      config.dir = *dir;
    } else if (auto trials = value("--trials=")) {
      config.trials = std::max(2, std::stoi(*trials));
    } else if (auto workers = value("--workers=")) {
      config.primary_workers = std::stoi(*workers);
    } else if (auto workload = value("--workload=")) {
      config.workload = *workload;
    } else if (arg == "--sweep") {
      config.sweep = true;
    } else if (auto filter = value("--filter=")) {
      config.filter = *filter != "none";
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      std::exit(2);
    }
  }
  if (config.primary_workers <= 0) {
    config.primary_workers = PrimaryWorkers();
  }
  return config;
}

int RunBenchmark(int argc, char** argv) {
  parquet::RegisterAll();
  avro::RegisterAll();
  const Config config = ParseArgs(argc, argv);
  std::cout << std::format(
      "hardware threads: {}, primary workers: {}, residual filter: {}, dir: {}\n",
      std::thread::hardware_concurrency(), config.primary_workers,
      config.filter ? "all-pass" : "none", config.dir.string());

  const std::vector<Workload> workloads = {
      {.name = "large",
       .files = 4,
       .rows_per_file = 2'000'000,
       .rows_per_row_group = 125'000,
       .target_split_size = int64_t{16} << 20},
      {.name = "small",
       .files = 400,
       .rows_per_file = 2'000,
       .rows_per_row_group = 2'000,
       .target_split_size = int64_t{128} << 20},
  };
  for (const auto& workload : workloads) {
    if (config.workload != "all" && config.workload != workload.name) {
      continue;
    }
    if (auto status = RunWorkload(config, workload); !status.has_value()) {
      std::cerr << "Workload " << workload.name << " failed: " << status.error().message
                << "\n";
      return 1;
    }
  }
  return 0;
}

}  // namespace
}  // namespace iceberg

int main(int argc, char** argv) { return iceberg::RunBenchmark(argc, argv); }
