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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/file_io.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> TestSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())}, 0);
}

Result<std::unique_ptr<TableMetadata>> NewMetadata(int version = 4) {
  return TableMetadata::Make(*TestSchema(), *PartitionSpec::Unpartitioned(),
                             *SortOrder::Unsorted(), "s3://bucket/table", {}, version);
}

// A complete v4 metadata fixture independent of the builder and serializer.
nlohmann::json MetadataJson() {
  return nlohmann::json::parse(R"({
    "format-version": 4,
    "table-uuid": "c9dc1a9f-6564-42ec-9a87-1db146e18019",
    "location": "s3://bucket/table",
    "last-sequence-number": 0,
    "last-updated-ms": 1000,
    "last-column-id": 1,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": [
      {"id": 1, "name": "id", "type": "long", "required": true}
    ]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "next-row-id": 0,
    "snapshots": []
  })");
}

nlohmann::json RootSnapshotJson() {
  return nlohmann::json::parse(R"({
    "snapshot-id": 1,
    "sequence-number": 1,
    "timestamp-ms": 1000,
    "root-manifest": "metadata/root.avro",
    "summary": {"operation": "append"},
    "schema-id": 0,
    "first-row-id": 0,
    "added-rows": 10
  })");
}

TEST(V4TableMetadataTest, CreateAndRoundTrip) {
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, NewMetadata());
  EXPECT_EQ(metadata->format_version, 4);
  EXPECT_EQ(metadata->next_row_id, 0);
  EXPECT_TRUE(metadata->snapshots.empty());
  EXPECT_FALSE(metadata->table_uuid.empty());
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*metadata));
  EXPECT_EQ(json["format-version"], 4);
  EXPECT_EQ(json["next-row-id"], 0);
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(json));
  EXPECT_EQ(*metadata, *parsed);
}

TEST(V4TableMetadataTest, CreateFromFormatVersionProperty) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, TableMetadata::Make(*TestSchema(), *PartitionSpec::Unpartitioned(),
                                         *SortOrder::Unsorted(), "s3://bucket/table",
                                         {{"format-version", "4"}, {"owner", "test"}}));
  EXPECT_EQ(metadata->format_version, 4);
  EXPECT_FALSE(metadata->properties.configs().contains("format-version"));
  EXPECT_EQ(metadata->properties.configs().at("owner"), "test");
  ICEBERG_UNWRAP_OR_FAIL(auto defaults,
                         NewMetadata(TableMetadata::kDefaultTableFormatVersion));
  EXPECT_EQ(defaults->format_version, 2);
}

TEST(V4TableMetadataTest, RejectUnsupportedVersionsWithoutNarrowing) {
  for (int version : {0, -1, 5, 260}) {
    SCOPED_TRACE(version);
    EXPECT_THAT(NewMetadata(version), IsError(ErrorKind::kInvalidArgument));
    auto json = MetadataJson();
    json["format-version"] = version;
    EXPECT_THAT(TableMetadataFromJson(json), HasErrorMessage("unsupported version"));
  }
  auto builder = TableMetadataBuilder::BuildFromEmpty(5);
  EXPECT_THAT(builder->Build(), HasErrorMessage("Unsupported table format version"));
  for (const auto& version : std::vector<nlohmann::json>{4294967300LL, 4.5, true}) {
    auto json = MetadataJson();
    json["format-version"] = version;
    EXPECT_FALSE(TableMetadataFromJson(json).has_value());
  }
}

TEST(V4TableMetadataTest, UpgradePreservesLegacySnapshots) {
  for (int version : {1, 2, 3}) {
    SCOPED_TRACE(version);
    ICEBERG_UNWRAP_OR_FAIL(auto base, NewMetadata(version));
    auto legacy_json = RootSnapshotJson();
    legacy_json.erase("root-manifest");
    legacy_json["manifest-list"] = "s3://bucket/table/metadata/legacy.avro";
    ICEBERG_UNWRAP_OR_FAIL(auto legacy_snapshot, SnapshotFromJson(legacy_json));
    base->snapshots.push_back(std::move(legacy_snapshot));
    base->next_row_id = 10;
    auto builder = TableMetadataBuilder::BuildFrom(base.get());
    builder->UpgradeFormatVersion(4);
    ICEBERG_UNWRAP_OR_FAIL(auto upgraded, builder->Build());
    EXPECT_EQ(upgraded->format_version, 4);
    EXPECT_EQ(upgraded->next_row_id, 10);
    EXPECT_EQ(upgraded->table_uuid, base->table_uuid);
    ASSERT_EQ(upgraded->snapshots.size(), 1);
    EXPECT_EQ(upgraded->snapshots.front()->manifest_list,
              legacy_json["manifest-list"].get<std::string>());
    EXPECT_FALSE(upgraded->snapshots.front()->root_manifest.has_value());
    ASSERT_EQ(builder->changes().size(), 1);
    EXPECT_EQ(builder->changes().front()->kind(),
              TableUpdate::Kind::kUpgradeFormatVersion);
    ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*upgraded));
    ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(json));
    EXPECT_EQ(*parsed, *upgraded);
    auto downgrade = TableMetadataBuilder::BuildFrom(upgraded.get());
    downgrade->UpgradeFormatVersion(3);
    EXPECT_THAT(downgrade->Build(), HasErrorMessage("Cannot downgrade"));
  }
}

TEST(V4TableMetadataTest, OptionalLocation) {
  auto json = MetadataJson();
  json.erase("location");
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(json));
  EXPECT_TRUE(parsed->location.empty());
  ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*parsed));
  EXPECT_FALSE(serialized.contains("location"));
  auto builder = TableMetadataBuilder::BuildFrom(parsed.get());
  builder->SetLocation("s3://catalog/table");
  ICEBERG_UNWRAP_OR_FAIL(auto located, builder->Build());
  EXPECT_EQ(located->location, "s3://catalog/table");
  for (int version : {1, 2, 3}) {
    json["format-version"] = version;
    EXPECT_FALSE(TableMetadataFromJson(json).has_value());
  }
}

TEST(V4TableMetadataTest, RejectRelativeTableLocationAndMissingRequiredFields) {
  auto json = MetadataJson();
  json["location"] = "relative/table";
  EXPECT_THAT(TableMetadataFromJson(json), HasErrorMessage("location must be absolute"));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, NewMetadata());
  auto builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->SetLocation("relative/table");
  EXPECT_THAT(builder->Build(), HasErrorMessage("location must be absolute"));
  metadata->location = "relative/table";
  EXPECT_THAT(ToJson(*metadata), HasErrorMessage("location must be absolute"));
  for (const auto* field : {"table-uuid", "next-row-id"}) {
    json = MetadataJson();
    json.erase(field);
    EXPECT_FALSE(TableMetadataFromJson(json).has_value());
  }
}

TEST(V4TableMetadataTest, RootManifestRoundTripAndExplicitReaderFailure) {
  auto json = MetadataJson();
  json["snapshots"] = nlohmann::json::array({RootSnapshotJson()});
  json["last-sequence-number"] = 1;
  json["next-row-id"] = 10;
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
  ASSERT_EQ(metadata->snapshots.size(), 1);
  auto snapshot = metadata->snapshots.front();
  EXPECT_EQ(snapshot->root_manifest, "metadata/root.avro");
  EXPECT_TRUE(snapshot->manifest_list.empty());
  ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*metadata));
  EXPECT_EQ(serialized["snapshots"][0], RootSnapshotJson());
  EXPECT_FALSE(serialized["snapshots"][0].contains("manifest-list"));
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(serialized));
  EXPECT_EQ(parsed->snapshots.front()->root_manifest, snapshot->root_manifest);
  EXPECT_THAT(SnapshotReader(snapshot.get()).Manifests(nullptr),
              IsError(ErrorKind::kNotSupported));
}

TEST(V4TableMetadataTest, RejectInvalidSnapshotRoots) {
  auto json = RootSnapshotJson();
  json["manifest-list"] = "legacy.avro";
  EXPECT_THAT(SnapshotFromJson(json), HasErrorMessage("exactly one"));
  for (const auto* field :
       {"root-manifest", "sequence-number", "first-row-id", "added-rows", "summary"}) {
    json = RootSnapshotJson();
    json.erase(field);
    EXPECT_FALSE(SnapshotFromJson(json).has_value()) << field;
  }
  json = MetadataJson();
  json["format-version"] = 3;
  json["snapshots"] = nlohmann::json::array({RootSnapshotJson()});
  EXPECT_THAT(TableMetadataFromJson(json), HasErrorMessage("requires format version 4"));
}

TEST(V4TableMetadataTest, NewSnapshotsRequireRootsAndMonotonicTimestamps) {
  auto json = MetadataJson();
  json["snapshots"] = nlohmann::json::array({RootSnapshotJson()});
  json["last-sequence-number"] = 1;
  json["next-row-id"] = 10;
  ICEBERG_UNWRAP_OR_FAIL(auto base, TableMetadataFromJson(json));
  auto child_json = RootSnapshotJson();
  child_json["snapshot-id"] = 2;
  child_json["parent-snapshot-id"] = 1;
  child_json["sequence-number"] = 2;
  child_json["first-row-id"] = 10;
  for (int64_t timestamp : {999, 1000, 1001}) {
    child_json["timestamp-ms"] = timestamp;
    ICEBERG_UNWRAP_OR_FAIL(auto child, SnapshotFromJson(child_json));
    auto builder = TableMetadataBuilder::BuildFrom(base.get());
    builder->AddSnapshot(std::move(child));
    if (timestamp <= 1000) {
      EXPECT_THAT(builder->Build(), IsError(ErrorKind::kRetryableValidationFailed));
    } else {
      ICEBERG_UNWRAP_OR_FAIL(auto updated, builder->Build());
      EXPECT_EQ(updated->next_row_id, 20);
    }
  }
  child_json.erase("root-manifest");
  child_json["manifest-list"] = "legacy.avro";
  child_json["timestamp-ms"] = 1001;
  ICEBERG_UNWRAP_OR_FAIL(auto child, SnapshotFromJson(child_json));
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->AddSnapshot(std::move(child));
  EXPECT_THAT(builder->Build(),
              HasErrorMessage("New v4 snapshots require root-manifest"));
}

TEST(V4TableMetadataTest, IgnoreLegacyIdentifiersAndRejectUnimplementedConstraints) {
  auto json = MetadataJson();
  json["schemas"][0]["identifier-field-ids"] = {999};
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  EXPECT_TRUE(schema->IdentifierFieldIds().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*metadata));
  EXPECT_FALSE(serialized["schemas"][0].contains("identifier-field-ids"));
  for (const auto* field : {"constraints", "last-constraint-id"}) {
    json = MetadataJson();
    json[field] = field == std::string_view("constraints")
                      ? nlohmann::json::array({{{"constraint-id", 1}}})
                      : nlohmann::json(1);
    EXPECT_THAT(TableMetadataFromJson(json), IsError(ErrorKind::kNotSupported));
  }
  json = MetadataJson();
  json["format-version"] = 3;
  json["schemas"][0]["identifier-field-ids"] = {1};
  ICEBERG_UNWRAP_OR_FAIL(auto base, TableMetadataFromJson(json));
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->UpgradeFormatVersion(4);
  EXPECT_THAT(builder->Build(), HasErrorMessage("primary-key constraint migration"));
  for (const auto& malformed_schema : std::vector<nlohmann::json>{nullptr, 1, "schema"}) {
    json = MetadataJson();
    json["schemas"] = nlohmann::json::array({malformed_schema});
    EXPECT_FALSE(TableMetadataFromJson(json).has_value());
  }
}

std::shared_ptr<MockFileIO> MemoryIO() {
  auto io = std::make_shared<::testing::NiceMock<MockFileIO>>();
  ON_CALL(*io, ReadFile)
      .WillByDefault(
          [ptr = io.get()](const std::string& path, std::optional<size_t> size) {
            return ptr->FileIO::ReadFile(path, size);
          });
  ON_CALL(*io, WriteFile)
      .WillByDefault([ptr = io.get()](const std::string& path, std::string_view data) {
        return ptr->FileIO::WriteFile(path, data);
      });
  return io;
}

TEST(V4TableMetadataTest, CatalogCreateLoadAndPropertyUpgrade) {
  auto io = MemoryIO();
  ICEBERG_UNWRAP_OR_FAIL(auto catalog,
                         InMemoryCatalog::Make("v4", io, "s3://warehouse", {}));
  for (const auto* initial_version : {"2", "4"}) {
    TableIdentifier ident{.name = std::string("version_") + initial_version};
    ICEBERG_UNWRAP_OR_FAIL(
        auto table,
        catalog->CreateTable(ident, TestSchema(), PartitionSpec::Unpartitioned(),
                             SortOrder::Unsorted(), "s3://warehouse/" + ident.name,
                             {{"format-version", initial_version}}));
    ICEBERG_UNWRAP_OR_FAIL(auto update, table->NewUpdateProperties());
    update->Set("format-version", "4");
    ASSERT_THAT(update->Commit(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog->LoadTable(ident));
    EXPECT_EQ(loaded->metadata()->format_version, 4);
    EXPECT_FALSE(loaded->metadata()->properties.configs().contains("format-version"));
    ICEBERG_UNWRAP_OR_FAIL(auto append, loaded->NewFastAppend());
    EXPECT_THAT(append->Commit(),
                HasErrorMessage("Writing v4 manifest trees is not supported"));
  }
}

TEST(V4TableMetadataTest, StagedCatalogCreate) {
  auto io = MemoryIO();
  ICEBERG_UNWRAP_OR_FAIL(auto catalog,
                         InMemoryCatalog::Make("v4", io, "s3://warehouse", {}));
  TableIdentifier ident{.name = "staged_v4"};
  ICEBERG_UNWRAP_OR_FAIL(
      auto transaction,
      catalog->StageCreateTable(ident, TestSchema(), PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "s3://warehouse/staged_v4",
                                {{"format-version", "4"}}));
  ASSERT_THAT(transaction->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog->LoadTable(ident));
  EXPECT_EQ(loaded->metadata()->format_version, 4);
}

TEST(V4TableMetadataTest, RejectInvalidVersionPropertiesAndIncompleteSnapshotWrites) {
  for (const auto* version : {"0", "-252", "5", "260", "invalid"}) {
    EXPECT_FALSE(TableMetadata::Make(*TestSchema(), *PartitionSpec::Unpartitioned(),
                                     *SortOrder::Unsorted(), "s3://bucket/table",
                                     {{"format-version", version}})
                     .has_value());
  }
  auto json = RootSnapshotJson();
  json["summary"] = nlohmann::json::object();
  EXPECT_FALSE(SnapshotFromJson(json).has_value());
  json = RootSnapshotJson();
  json["sequence-number"] = 0;
  EXPECT_FALSE(SnapshotFromJson(json).has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, NewMetadata());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, SnapshotFromJson(RootSnapshotJson()));
  snapshot->first_row_id.reset();
  metadata->snapshots.push_back(std::move(snapshot));
  EXPECT_THAT(ToJson(*metadata), HasErrorMessage("incomplete v4 snapshot"));
}

}  // namespace
}  // namespace iceberg
