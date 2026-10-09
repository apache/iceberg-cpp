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

#include "iceberg/catalog/hive/hive_utils.h"

#include <gtest/gtest.h>

namespace iceberg::hive {

TEST(ValidateNamespaceTest, SingleLevelAccepted) {
  EXPECT_TRUE(ValidateNamespace(Namespace{{"warehouse"}}).has_value());
}

TEST(ValidateNamespaceTest, MultiLevelRejected) {
  auto status = ValidateNamespace(Namespace{{"a", "b"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ValidateNamespaceTest, EmptyLevelRejected) {
  auto status = ValidateNamespace(Namespace{{""}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ValidateOwnerSettingsTest, OwnerTypeRequiresOwner) {
  auto status = ValidateOwnerSettings({{"hive.metastore.database.owner-type", "USER"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ValidateOwnerSettingsTest, OwnerOrOwnerPlusTypeAreOk) {
  EXPECT_TRUE(
      ValidateOwnerSettings({{"hive.metastore.database.owner", "alice"}}).has_value());
  EXPECT_TRUE(ValidateOwnerSettings({{"hive.metastore.database.owner", "alice"},
                                     {"hive.metastore.database.owner-type", "ROLE"}})
                  .has_value());
  EXPECT_TRUE(ValidateOwnerSettings({}).has_value());
}

TEST(ValidateOwnerSettingsTest, UnknownOwnerTypeRejected) {
  auto status = ValidateOwnerSettings({{"hive.metastore.database.owner", "alice"},
                                       {"hive.metastore.database.owner-type", "ADMIN"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ValidateOwnerSettingsTest, BareOwnerKeysAreOrdinaryProperties) {
  EXPECT_TRUE(ValidateOwnerSettings({{"owner-type", "nonsense"}}).has_value());
}

TEST(ConvertToHiveDatabaseTest, LiftsReservedKeysIntoDedicatedFields) {
  auto db = ConvertToHiveDatabase(Namespace{{"warehouse"}},
                                  {{"comment", "primary db"},
                                   {"location", "s3://bucket/wh/"},
                                   {"hive.metastore.database.owner", "alice"},
                                   {"hive.metastore.database.owner-"
                                    "type",
                                    "USER"},
                                   {"custom-prop", "value"}});
  ASSERT_TRUE(db.has_value()) << db.error().message;
  EXPECT_EQ(db->name, "warehouse");
  EXPECT_EQ(db->description, "primary db");
  EXPECT_EQ(db->location_uri, "s3://bucket/wh/");
  EXPECT_EQ(db->owner_name, "alice");
  EXPECT_EQ(db->owner_type, "USER");
  ASSERT_EQ(db->parameters.size(), 1);
  EXPECT_EQ(db->parameters.at("custom-prop"), "value");
}

TEST(ConvertToHiveDatabaseTest, RejectsHierarchicalNamespace) {
  auto db = ConvertToHiveDatabase(Namespace{{"a", "b"}}, {});
  ASSERT_FALSE(db.has_value());
  EXPECT_EQ(db.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ConvertToHiveDatabaseTest, RejectsNonCanonicalOwnerType) {
  auto db = ConvertToHiveDatabase(Namespace{{"warehouse"}},
                                  {{"hive.metastore.database.owner", "alice"},
                                   {"hive.metastore.database.owner-type", "role"}});
  ASSERT_FALSE(db.has_value());
  EXPECT_EQ(db.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ConvertFromHiveDatabaseTest, RoundTripWithCustomProperties) {
  auto original = ConvertToHiveDatabase(Namespace{{"warehouse"}},
                                        {{"comment", "primary"},
                                         {"hive.metastore.database.owner", "alice"},
                                         {"team", "data"}});
  ASSERT_TRUE(original.has_value()) << original.error().message;

  const auto round_tripped = ConvertFromHiveDatabase(*original);
  EXPECT_EQ(round_tripped.ns.levels, std::vector<std::string>{"warehouse"});
  EXPECT_EQ(round_tripped.properties.at("comment"), "primary");
  EXPECT_EQ(round_tripped.properties.at("hive.metastore.database.owner"), "alice");
  EXPECT_EQ(round_tripped.properties.at("team"), "data");
  EXPECT_FALSE(round_tripped.properties.contains("hive.metastore.database.owner-type"));
}

TEST(ConvertToHiveTableTest, SetsIcebergMarkerParametersAndStorageDescriptor) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  const std::vector<HiveColumn> columns = {{.name = "id", .type_string = "bigint"},
                                           {.name = "amount", .type_string = "double"}};
  auto table = ConvertToHiveTable(ident, columns,
                                  /*metadata_location=*/
                                  "s3://bucket/wh/warehouse.db/"
                                  "orders/metadata/v1.metadata.json",
                                  /*location=*/"s3://bucket/wh/warehouse.db/orders",
                                  /*table_properties=*/{{"format-version", "2"}});
  ASSERT_TRUE(table.has_value()) << table.error().message;
  EXPECT_EQ(table->db_name, "warehouse");
  EXPECT_EQ(table->table_name, "orders");
  EXPECT_EQ(table->table_type, "EXTERNAL_TABLE");
  EXPECT_EQ(table->location, "s3://bucket/wh/warehouse.db/orders");
  EXPECT_EQ(table->columns.size(), 2);
  EXPECT_EQ(table->columns[0].name, "id");
  EXPECT_EQ(table->columns[0].type_string, "bigint");

  EXPECT_EQ(table->parameters.at("metadata_location"),
            "s3://bucket/wh/warehouse.db/orders/metadata/v1.metadata.json");
  EXPECT_EQ(table->parameters.at("table_type"), "ICEBERG");
  EXPECT_EQ(table->parameters.at("EXTERNAL"), "TRUE");
  EXPECT_EQ(table->parameters.at("format-version"), "2");
  EXPECT_FALSE(table->parameters.contains("external.table.purge"));

  EXPECT_EQ(table->serde, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
  EXPECT_EQ(table->input_format, "org.apache.hadoop.mapred.FileInputFormat");
  EXPECT_EQ(table->output_format, "org.apache.hadoop.mapred.FileOutputFormat");
}

TEST(ConvertToHiveTableTest, UsesQualifiedOwnerAndPreservesCommentAndBareOwner) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  auto table = ConvertToHiveTable(ident, /*columns=*/{}, /*metadata_location=*/"loc",
                                  /*location=*/"root",
                                  {{"comment", "table description"},
                                   {"owner", "bob"},
                                   {"hive.metastore.table.owner", "alice"},
                                   {"location", "other root"}});
  ASSERT_TRUE(table.has_value()) << table.error().message;
  EXPECT_EQ(table->owner, "alice");
  EXPECT_EQ(table->location, "root");
  EXPECT_FALSE(table->parameters.contains("hive.metastore.table.owner"));
  EXPECT_EQ(table->parameters.at("location"), "other root");
  EXPECT_EQ(table->parameters.at("comment"), "table description");
  EXPECT_EQ(table->parameters.at("owner"), "bob");
}

TEST(ConvertToHiveTableTest, TranslatesGcEnabled) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  for (const std::string value : {"true", "false"}) {
    SCOPED_TRACE(value);
    auto table = ConvertToHiveTable(ident, {}, "loc", "root", {{"gc.enabled", value}});
    ASSERT_TRUE(table.has_value()) << table.error().message;
    EXPECT_EQ(table->parameters.at("external.table.purge"), value);
    EXPECT_FALSE(table->parameters.contains("gc.enabled"));
  }
}

TEST(ConvertToHiveTableTest, ConflictingPurgePropertiesFollowIterationOrder) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  const std::unordered_map<std::string, std::string> properties = {
      {"gc.enabled", "false"}, {"external.table.purge", "true"}};
  std::string expected;
  for (const auto& [key, value] : properties) {
    expected = value;
  }
  auto table = ConvertToHiveTable(ident, {}, "loc", "root", properties);
  ASSERT_TRUE(table.has_value()) << table.error().message;
  EXPECT_EQ(table->parameters.at("external.table.purge"), expected);
}

TEST(ConvertToHiveTableTest, PreservesExplicitPurgePropertyWithoutGcEnabled) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  auto table =
      ConvertToHiveTable(ident, {}, "loc", "root", {{"external.table.purge", "true"}});
  ASSERT_TRUE(table.has_value()) << table.error().message;
  EXPECT_EQ(table->parameters.at("external.table.purge"), "true");
}

TEST(ConvertToHiveTableTest, RejectsEmptyMetadataLocation) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  auto table = ConvertToHiveTable(ident, {}, "", "root", {});
  ASSERT_FALSE(table.has_value());
  EXPECT_EQ(table.error().kind, ErrorKind::kInvalidArgument);
}

TEST(ConvertToHiveTableTest, ProtectsIcebergMarkerParameters) {
  TableIdentifier ident{.ns = Namespace{{"warehouse"}}, .name = "orders"};
  auto table = ConvertToHiveTable(
      ident, {}, "loc", "root",
      {{"metadata_location", "other"}, {"table_type", "OTHER"}, {"EXTERNAL", "FALSE"}});
  ASSERT_TRUE(table.has_value()) << table.error().message;
  EXPECT_EQ(table->parameters.at("metadata_location"), "loc");
  EXPECT_EQ(table->parameters.at("table_type"), "ICEBERG");
  EXPECT_EQ(table->parameters.at("EXTERNAL"), "TRUE");
}

TEST(GetMetadataLocationTest, ExtractsKnownKey) {
  auto loc = GetMetadataLocation({{"metadata_location", "s3://m.json"}});
  ASSERT_TRUE(loc.has_value()) << loc.error().message;
  EXPECT_EQ(*loc, "s3://m.json");
}

TEST(GetMetadataLocationTest, AbsenceMapsToNotFound) {
  auto loc = GetMetadataLocation({{"EXTERNAL", "TRUE"}});
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kNotFound);
}

TEST(GetMetadataLocationTest, EmptyLocationMapsToNotFound) {
  auto loc = GetMetadataLocation({{"metadata_location", ""}});
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kNotFound);
}

TEST(ValidateIcebergTableTest, AcceptsCaseInsensitiveIcebergMarker) {
  const TableIdentifier id{.ns = Namespace{{"sales"}}, .name = "orders"};
  EXPECT_TRUE(ValidateIcebergTable(id, {{"table_type", "ICEBERG"}}).has_value());
  EXPECT_TRUE(ValidateIcebergTable(id, {{"table_type", "iceberg"}}).has_value());
  EXPECT_TRUE(ValidateIcebergTable(id, {{"table_type", "Iceberg"}}).has_value());
}

TEST(ValidateIcebergTableTest, RejectsMissingMarker) {
  const TableIdentifier id{.ns = Namespace{{"sales"}}, .name = "orders"};
  auto status = ValidateIcebergTable(id, {{"EXTERNAL", "TRUE"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kNoSuchTable);
}

TEST(ValidateIcebergTableTest, RejectsForeignTableType) {
  const TableIdentifier id{.ns = Namespace{{"sales"}}, .name = "orders"};
  auto status =
      ValidateIcebergTable(id, {{"table_type", "MANAGED_TABLE"}, {"EXTERNAL", "TRUE"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kNoSuchTable);
}

TEST(GetDefaultTableLocationTest, TrimsTrailingSlashAndAppendsDbAndTable) {
  EXPECT_EQ(
      GetDefaultTableLocation("s3://bucket/wh/", Namespace{{"warehouse"}}, "orders"),
      "s3://bucket/wh/warehouse.db/orders");
  EXPECT_EQ(GetDefaultTableLocation("hdfs://nn/data", Namespace{{"sales"}}, "orders"),
            "hdfs://nn/data/sales.db/orders");
}

}  // namespace iceberg::hive
