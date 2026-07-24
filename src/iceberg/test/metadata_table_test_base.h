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

/// \file metadata_table_test_base.h
/// Shared test base for all metadata table tests.
///
/// Provides common helpers (FinishAndImport, MakeTestSnapshots,
/// MakeTableWithSnapshots) and the MockFileIO + MockCatalog fixture that
/// every metadata table test needs.

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Base class for all metadata table tests.
///
/// Provides MockFileIO and MockCatalog instances plus helpers shared across
/// metadata table tests (SnapshotsTable, HistoryTable, RefsTable, ...).
class MetadataTableTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    io_ = std::make_shared<MockFileIO>();
    catalog_ = std::make_shared<MockCatalog>();

    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});

    TableIdentifier source_ident{.ns = Namespace{.levels = {"db"}},
                                 .name = "source_table"};
    ICEBERG_UNWRAP_OR_FAIL(table_, Table::Make(source_ident, metadata_,
                                               "s3://bucket/meta.json", io_, catalog_));
  }

  /// \brief Import a Scan()-produced ArrowArray into an Arrow RecordBatch.
static std::shared_ptr<::arrow::RecordBatch> FinishAndImport(ArrowArray&& array,
                                                               const Schema& schema) {
    ArrowSchema c_schema{};
    EXPECT_THAT(ToArrowSchema(schema, &c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();

    // ImportRecordBatch takes ownership of the array and releases it.
    return ::arrow::ImportRecordBatch(&array, arrow_schema).ValueOrDie();
  }

  /// \brief Create two snapshots matching the Java TestDataTaskParser test data.
  ///
  /// Snapshot 1: id=1, no parent, timestamp=1234567890000, operation="append"
  /// Snapshot 2: id=2, parent=1, timestamp=9876543210000, operation="append"
  static std::pair<std::shared_ptr<Snapshot>, std::shared_ptr<Snapshot>>
  MakeTestSnapshots() {
    std::unordered_map<std::string, std::string> summary1{
        {"added-data-files", "1"},       {"added-records", "1"},
        {"added-files-size", "10"},      {"changed-partition-count", "1"},
        {"total-records", "1"},          {"total-files-size", "10"},
        {"total-data-files", "1"},       {"total-delete-files", "0"},
        {"total-position-deletes", "0"}, {"total-equality-deletes", "0"},
        {"operation", "append"},
    };

    std::unordered_map<std::string, std::string> summary2{
        {"added-data-files", "1"},       {"added-records", "1"},
        {"added-files-size", "10"},      {"changed-partition-count", "1"},
        {"total-records", "2"},          {"total-files-size", "20"},
        {"total-data-files", "2"},       {"total-delete-files", "0"},
        {"total-position-deletes", "0"}, {"total-equality-deletes", "0"},
        {"operation", "append"},
    };

    auto snap1 = std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = 1,
        .parent_snapshot_id = std::nullopt,
        .sequence_number = 1,
        .timestamp_ms = TimePointMsFromUnixMs(1234567890000),
        .manifest_list = "file:/tmp/manifest1.avro",
        .summary = std::move(summary1),
        .schema_id = 1,
    });

    auto snap2 = std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = 2,
        .parent_snapshot_id = 1,
        .sequence_number = 2,
        .timestamp_ms = TimePointMsFromUnixMs(9876543210000),
        .manifest_list = "file:/tmp/manifest2.avro",
        .summary = std::move(summary2),
        .schema_id = 1,
    });

    return {snap1, snap2};
  }

  /// \brief Create a Table with the given snapshots.
  Result<std::shared_ptr<Table>> MakeTableWithSnapshots(
      std::vector<std::shared_ptr<Snapshot>> snapshots, int64_t current_snapshot_id) {
    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    auto metadata = std::make_shared<TableMetadata>(TableMetadata{
        .format_version = 2,
        .schemas = {schema},
        .current_schema_id = 1,
        .current_snapshot_id = current_snapshot_id,
        .snapshots = std::move(snapshots),
    });

    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "test_table"};
    return Table::Make(ident, metadata, "s3://bucket/meta.json", io_, catalog_);
  }

  std::shared_ptr<MockFileIO> io_;
  std::shared_ptr<MockCatalog> catalog_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<Table> table_;
};

}  // namespace iceberg
