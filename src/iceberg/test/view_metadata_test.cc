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

#include "iceberg/view_metadata.h"

#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

ViewRepresentation SqlRepr(std::string sql, std::string dialect) {
  return SqlViewRepresentation{.sql = std::move(sql), .dialect = std::move(dialect)};
}

std::shared_ptr<Schema> MakeSchema(int32_t schema_id) {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "customer_id", int64()),
                               SchemaField::MakeOptional(2, "country", string())},
      schema_id);
}

ViewVersion MakeVersion(int32_t version_id, int32_t schema_id,
                        std::vector<ViewRepresentation> representations) {
  return ViewVersion{
      .version_id = version_id,
      .schema_id = schema_id,
      .timestamp_ms = TimePointMs{std::chrono::milliseconds(1600000000000)},
      .summary = {{"engine-name", "test"}},
      .representations = std::move(representations),
      .default_catalog = "prod",
      .default_namespace = Namespace{{"analytics"}},
  };
}

ViewMetadata MakeValidMetadata() {
  ViewMetadata metadata;
  metadata.format_version = ViewMetadata::kDefaultViewFormatVersion;
  metadata.view_uuid = "fa6506c3-7681-40c8-86dc-e36561f83385";
  metadata.location = "s3://bucket/warehouse/analytics.db/active_customers";
  metadata.schemas = {MakeSchema(0)};
  metadata.current_version_id = 0;
  metadata.versions = {std::make_shared<ViewVersion>(
      MakeVersion(0, 0, {SqlRepr("SELECT customer_id FROM customers", "spark")}))};
  metadata.version_log = {ViewHistoryEntry{
      .timestamp_ms = TimePointMs{std::chrono::milliseconds(1600000000000)},
      .version_id = 0,
  }};
  metadata.properties = {{"comment", "active customers"}};
  return metadata;
}

TEST(ViewMetadataTest, ValidateAcceptsValidMetadata) {
  ViewMetadata metadata = MakeValidMetadata();
  EXPECT_THAT(metadata.Validate(), IsOk());

  auto current = metadata.CurrentVersion();
  ASSERT_THAT(current, IsOk());
  EXPECT_EQ(current->get()->version_id, 0);

  auto current_schema = metadata.CurrentSchema();
  ASSERT_THAT(current_schema, IsOk());
  EXPECT_EQ(current_schema->get()->schema_id(), 0);
}

TEST(ViewMetadataTest, ValidateAcceptsUnknownRepresentation) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations.push_back(
      UnknownViewRepresentation{.type = "cypher"});
  EXPECT_THAT(metadata.Validate(), IsOk());
}

TEST(ViewMetadataTest, ValidateAcceptsMultipleVersionsAndDialects) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.schemas.push_back(MakeSchema(1));
  metadata.current_version_id = 1;
  metadata.versions[0]->representations.push_back(
      SqlRepr("SELECT customer_id, country FROM customers WHERE active = true", "trino"));
  metadata.versions.push_back(std::make_shared<ViewVersion>(MakeVersion(
      1, 1,
      {SqlRepr("SELECT customer_id, country FROM customers WHERE active = true",
               "Trino")})));
  metadata.version_log.push_back(ViewHistoryEntry{
      .timestamp_ms = TimePointMs{std::chrono::milliseconds(1600000001000)},
      .version_id = 1,
  });
  EXPECT_THAT(metadata.Validate(), IsOk());

  auto current_schema = metadata.CurrentSchema();
  ASSERT_THAT(current_schema, IsOk());
  EXPECT_EQ(current_schema->get()->schema_id(), 1);
}

TEST(ViewMetadataTest, ValidateRejectsUnsupportedFormatVersion) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.format_version = 2;
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsEmptyUuid) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.view_uuid.clear();
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsEmptyLocation) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.location.clear();
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsMissingSchemas) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.schemas.clear();
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsNullSchema) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.schemas.push_back(nullptr);
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsDuplicateSchemaIds) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.schemas.push_back(MakeSchema(0));
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsMissingVersions) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions.clear();
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsNullVersion) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions.push_back(nullptr);
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsDuplicateVersionIds) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions.push_back(std::make_shared<ViewVersion>(
      MakeVersion(0, 0, {SqlRepr("SELECT customer_id FROM customers", "trino")})));
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsUnlistedCurrentVersion) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.current_version_id = 7;
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));

  auto current = metadata.CurrentVersion();
  EXPECT_THAT(current, IsError(ErrorKind::kNotFound));
}

TEST(ViewMetadataTest, ValidateRejectsUnlistedSchemaReference) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->schema_id = 5;
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsEmptyDefaultNamespace) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->default_namespace = Namespace{};
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsDuplicateDialects) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations.push_back(
      SqlRepr("SELECT customer_id FROM customers", "spark"));
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsDuplicateDialectsWithCaseFolding) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations.push_back(
      SqlRepr("SELECT customer_id FROM customers", "SPARK"));
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsSqlRepresentationWithoutSql) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations = {SqlRepr("", "spark")};
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsSqlRepresentationWithoutDialect) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations = {SqlRepr("SELECT 1", "")};
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, ValidateRejectsUnknownRepresentationWithoutType) {
  ViewMetadata metadata = MakeValidMetadata();
  metadata.versions[0]->representations = {UnknownViewRepresentation{.type = ""}};
  EXPECT_THAT(metadata.Validate(), IsError(ErrorKind::kInvalid));
}

TEST(ViewMetadataTest, AccessorsReturnNotFoundForMissingEntries) {
  ViewMetadata metadata = MakeValidMetadata();
  EXPECT_THAT(metadata.VersionById(999), IsError(ErrorKind::kNotFound));
  EXPECT_THAT(metadata.SchemaById(999), IsError(ErrorKind::kNotFound));

  metadata.versions.push_back(nullptr);
  EXPECT_THAT(metadata.VersionById(999), IsError(ErrorKind::kNotFound));
}

TEST(ViewMetadataTest, EqualityComparesAllFields) {
  ViewMetadata lhs = MakeValidMetadata();
  ViewMetadata rhs = MakeValidMetadata();
  EXPECT_EQ(lhs, rhs);

  rhs.properties.at("comment") = "changed";
  EXPECT_NE(lhs, rhs);

  ViewMetadata changed = MakeValidMetadata();
  changed.versions[0]->representations.push_back(
      UnknownViewRepresentation{.type = "cypher"});
  EXPECT_NE(lhs, changed);

  ViewMetadata unknown = lhs;
  unknown.versions[0]->representations.push_back(
      UnknownViewRepresentation{.type = "cypher"});
  EXPECT_EQ(unknown, changed);
}

TEST(ViewVersionTest, EqualityComparesAllFields) {
  ViewVersion lhs = MakeVersion(0, 0, {SqlRepr("SELECT 1", "spark")});
  ViewVersion rhs = MakeVersion(0, 0, {SqlRepr("SELECT 1", "spark")});
  EXPECT_EQ(lhs, rhs);

  rhs.representations[0] = SqlRepr("SELECT 1", "trino");
  EXPECT_NE(lhs, rhs);

  rhs.representations[0] = SqlRepr("SELECT 2", "spark");
  EXPECT_NE(lhs, rhs);
}

TEST(ViewRepresentationTest, Helpers) {
  const ViewRepresentation sql = SqlRepr("SELECT 1", "spark");
  const auto* sql_repr = GetSql(sql);
  ASSERT_NE(sql_repr, nullptr);
  EXPECT_EQ(sql_repr->dialect, "spark");
  EXPECT_EQ(RepresentationType(sql), kSqlRepresentationType);

  const ViewRepresentation unknown = UnknownViewRepresentation{.type = "cypher"};
  EXPECT_EQ(GetSql(unknown), nullptr);
  EXPECT_EQ(RepresentationType(unknown), "cypher");
}

}  // namespace
}  // namespace iceberg
