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

#include <iostream>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/table.h"
#include "iceberg/table_scan.h"

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " <warehouse_location> <table_name> <table_location>" << std::endl;
    return 0;
  }

  const std::string warehouse_location = argv[1];
  const std::string table_name = argv[2];
  const std::string table_location = argv[3];
  const std::unordered_map<std::string, std::string> properties;

  iceberg::avro::RegisterAll();
  iceberg::parquet::RegisterAll();

  auto catalog = iceberg::InMemoryCatalog::Make("test", iceberg::arrow::MakeLocalFileIO(),
                                                warehouse_location, properties);

  auto register_result = catalog->RegisterTable({.name = table_name}, table_location);
  if (!register_result.has_value()) {
    std::cerr << "Failed to register table: " << register_result.error().message
              << std::endl;
    return 1;
  }

  auto load_result = catalog->LoadTable({.name = table_name});
  if (!load_result.has_value()) {
    std::cerr << "Failed to load table: " << load_result.error().message << std::endl;
    return 1;
  }

  auto table = std::move(load_result.value());
  auto scan_builder = table->NewScan();
  if (!scan_builder.has_value()) {
    std::cerr << "Failed to create scan builder: " << scan_builder.error().message
              << std::endl;
    return 1;
  }
  auto scan_result = scan_builder.value()->Build();
  if (!scan_result.has_value()) {
    std::cerr << "Failed to build scan: " << scan_result.error().message << std::endl;
    return 1;
  }

  auto scan = std::move(scan_result.value());
  auto plan_result = scan->PlanFiles();
  if (!plan_result.has_value()) {
    std::cerr << "Failed to plan files: " << plan_result.error().message << std::endl;
    return 1;
  }

  std::cout << "Scan tasks: " << std::endl;
  auto scan_tasks = std::move(plan_result.value());
  for (const auto& scan_task : scan_tasks) {
    std::cout << " - " << scan_task->data_file()->file_path << std::endl;
  }

  return 0;
}
