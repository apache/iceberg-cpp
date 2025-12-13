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
#include <iostream>
#include <memory>
#include <string>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/type.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_reader.h"
#include "iceberg/schema.h"

void PrintUsage(const char* program_name) {
  std::cerr << "Usage: " << program_name << " [options] <avro_file>\n"
            << "Options:\n"
            << "  --skip-datum=<true|false>  Use direct decoder (default: true)\n"
            << "  --batch-size=<N>           Batch size for reading (default: 4096)\n"
            << "  --help                     Show this help message\n"
            << "\nExample:\n"
            << "  " << program_name
            << " --skip-datum=false --batch-size=1000 data.avro\n";
}

int main(int argc, char* argv[]) {
  iceberg::avro::RegisterAll();

  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }

  std::string avro_file;
  bool skip_datum = true;
  int64_t batch_size = 4096;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if (arg.starts_with("--skip-datum=")) {
      std::string value = arg.substr(13);
      if (value == "true" || value == "1") {
        skip_datum = true;
      } else if (value == "false" || value == "0") {
        skip_datum = false;
      } else {
        std::cerr << "Invalid value for --skip-datum: " << value << "\n";
        return 1;
      }
    } else if (arg.starts_with("--batch-size=")) {
      batch_size = std::stoll(arg.substr(13));
      if (batch_size <= 0) {
        std::cerr << "Batch size must be positive\n";
        return 1;
      }
    } else if (arg[0] == '-') {
      std::cerr << "Unknown option: " << arg << "\n";
      PrintUsage(argv[0]);
      return 1;
    } else {
      avro_file = arg;
    }
  }

  if (avro_file.empty()) {
    std::cerr << "Error: No Avro file specified\n";
    PrintUsage(argv[0]);
    return 1;
  }

  std::cout << "Scanning Avro file: " << avro_file << "\n";
  std::cout << "Skip datum: " << (skip_datum ? "true" : "false") << "\n";
  std::cout << "Batch size: " << batch_size << "\n";
  std::cout << std::string(60, '-') << "\n";

  auto local_fs = std::make_shared<::arrow::fs::LocalFileSystem>();
  auto file_io = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs);

  // Get file info
  auto file_info_result = local_fs->GetFileInfo(avro_file);
  if (!file_info_result.ok()) {
    std::cerr << "Error: Cannot access file: " << file_info_result.status().message()
              << "\n";
    return 1;
  }
  auto file_info = file_info_result.ValueOrDie();
  if (file_info.type() != ::arrow::fs::FileType::File) {
    std::cerr << "Error: Not a file: " << avro_file << "\n";
    return 1;
  }

  std::cout << "File size: " << file_info.size() << " bytes\n";

  // Configure reader properties
  auto reader_properties = iceberg::ReaderProperties::default_properties();
  reader_properties->Set(iceberg::ReaderProperties::kAvroSkipDatum, skip_datum);
  reader_properties->Set(iceberg::ReaderProperties::kBatchSize, batch_size);

  // Open reader (without projection to read all columns)
  auto reader_result = iceberg::ReaderFactoryRegistry::Open(
      iceberg::FileFormatType::kAvro, {.path = avro_file,
                                       .length = file_info.size(),
                                       .io = file_io,
                                       .projection = nullptr,
                                       .properties = std::move(reader_properties)});

  if (!reader_result.has_value()) {
    std::cerr << "Error opening reader: " << reader_result.error().message << "\n";
    return 1;
  }

  auto reader = std::move(reader_result.value());

  // Get schema
  auto schema_result = reader->Schema();
  if (!schema_result.has_value()) {
    std::cerr << "Error getting schema: " << schema_result.error().message << "\n";
    return 1;
  }
  auto arrow_schema = schema_result.value();
  auto arrow_schema_import = ::arrow::ImportType(&arrow_schema);
  if (!arrow_schema_import.ok()) {
    std::cerr << "Error importing schema: " << arrow_schema_import.status().message()
              << "\n";
    return 1;
  }
  std::cout << "Schema: " << arrow_schema_import.ValueOrDie()->ToString() << "\n";
  std::cout << std::string(60, '-') << "\n";

  // Scan file and measure time
  auto start = std::chrono::high_resolution_clock::now();

  int64_t total_rows = 0;
  int64_t batch_count = 0;

  while (true) {
    auto batch_result = reader->Next();
    if (!batch_result.has_value()) {
      std::cerr << "Error reading batch: " << batch_result.error().message << "\n";
      return 1;
    }

    auto batch_opt = batch_result.value();
    if (!batch_opt.has_value()) {
      // End of file
      break;
    }

    auto arrow_array = batch_opt.value();
    auto arrow_type = arrow_schema_import.ValueOrDie();
    auto array_import = ::arrow::ImportArray(&arrow_array, arrow_type);
    if (!array_import.ok()) {
      std::cerr << "Error importing array: " << array_import.status().message() << "\n";
      return 1;
    }

    int64_t batch_rows = array_import.ValueOrDie()->length();
    total_rows += batch_rows;
    batch_count++;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Print results
  std::cout << "\nResults:\n";
  std::cout << "  Total rows: " << total_rows << "\n";
  std::cout << "  Batches: " << batch_count << "\n";
  std::cout << "  Time: " << duration.count() << " ms\n";
  std::cout << "  Throughput: "
            << (duration.count() > 0 ? (total_rows * 1000 / duration.count()) : 0)
            << " rows/sec\n";
  std::cout << "  Throughput: "
            << (duration.count() > 0
                    ? (file_info.size() / 1024.0 / 1024.0) / (duration.count() / 1000.0)
                    : 0)
            << " MB/sec\n";

  return 0;
}
