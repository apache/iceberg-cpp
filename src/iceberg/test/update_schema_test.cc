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

#include "iceberg/update/update_schema.h"

#include <memory>

#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg {

using internal::checked_cast;

class UpdateSchemaTest : public UpdateTestBase {};

TEST_F(UpdateSchemaTest, AddOptionalColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col", int32(), "A new integer column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt, result.schema->FindFieldByName("new_col"));
  ASSERT_TRUE(new_field_opt.has_value());

  const auto& new_field = new_field_opt->get();
  EXPECT_EQ(new_field.name(), "new_col");
  EXPECT_EQ(new_field.type(), int32());
  EXPECT_TRUE(new_field.optional());
  EXPECT_EQ(new_field.doc(), "A new integer column");
}

TEST_F(UpdateSchemaTest, AddRequiredColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddRequiredColumn("required_col", string(), "A required string column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Incompatible change"));
}

TEST_F(UpdateSchemaTest, AddRequiredColumnWithAllowIncompatible) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges().AddRequiredColumn("required_col", string(),
                                                       "A required string column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt,
                         result.schema->FindFieldByName("required_col"));
  ASSERT_TRUE(new_field_opt.has_value());

  const auto& new_field = new_field_opt->get();
  EXPECT_EQ(new_field.name(), "required_col");
  EXPECT_EQ(new_field.type(), string());
  EXPECT_FALSE(new_field.optional());
  EXPECT_EQ(new_field.doc(), "A required string column");
}

TEST_F(UpdateSchemaTest, AddMultipleColumns) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col1", int32(), "First column")
      .AddColumn("col2", string(), "Second column")
      .AddColumn("col3", boolean(), "Third column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto col1_opt, result.schema->FindFieldByName("col1"));
  ICEBERG_UNWRAP_OR_FAIL(auto col2_opt, result.schema->FindFieldByName("col2"));
  ICEBERG_UNWRAP_OR_FAIL(auto col3_opt, result.schema->FindFieldByName("col3"));

  ASSERT_TRUE(col1_opt.has_value());
  ASSERT_TRUE(col2_opt.has_value());
  ASSERT_TRUE(col3_opt.has_value());

  EXPECT_EQ(col1_opt->get().type(), int32());
  EXPECT_EQ(col2_opt->get().type(), string());
  EXPECT_EQ(col3_opt->get().type(), boolean());
}

TEST_F(UpdateSchemaTest, AddColumnWithDotInNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col.with.dots", int32(), "Column with dots");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add column with ambiguous name"));
}

TEST_F(UpdateSchemaTest, AddColumnToNestedStruct) {
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "nested_field", int32(), true, "Nested field")});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type, "A struct column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "new_nested_field", string(), "New nested field");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  ASSERT_TRUE(struct_field.type()->is_nested());

  const auto& nested_struct = checked_cast<const StructType&>(*struct_field.type());
  ICEBERG_UNWRAP_OR_FAIL(auto nested_field_opt,
                         nested_struct.GetFieldByName("new_nested_field"));
  ASSERT_TRUE(nested_field_opt.has_value());

  const auto& nested_field = nested_field_opt->get();
  EXPECT_EQ(nested_field.name(), "new_nested_field");
  EXPECT_EQ(nested_field.type(), string());
}

TEST_F(UpdateSchemaTest, AddColumnToNonExistentParentFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("non_existent_parent", "new_field", int32(), "New field");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot find parent struct"));
}

TEST_F(UpdateSchemaTest, AddColumnToNonStructParentFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("primitive_col", int32(), "A primitive column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("primitive_col", "nested_field", string(), "Should fail");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add to non-struct column"));
}

TEST_F(UpdateSchemaTest, AddDuplicateColumnNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("duplicate_col", int32(), "First column")
      .AddColumn("duplicate_col", string(), "Duplicate column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Duplicate path found"));
}

TEST_F(UpdateSchemaTest, ColumnIdAssignment) {
  ICEBERG_UNWRAP_OR_FAIL(auto original_schema, table_->schema());
  int32_t original_last_id = table_->metadata()->last_column_id;

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col1", int32(), "First new column")
      .AddColumn("new_col2", string(), "Second new column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  EXPECT_EQ(result.new_last_column_id, original_last_id + 2);

  ICEBERG_UNWRAP_OR_FAIL(auto col1_opt, result.schema->FindFieldByName("new_col1"));
  ICEBERG_UNWRAP_OR_FAIL(auto col2_opt, result.schema->FindFieldByName("new_col2"));

  ASSERT_TRUE(col1_opt.has_value());
  ASSERT_TRUE(col2_opt.has_value());

  EXPECT_EQ(col1_opt->get().field_id(), original_last_id + 1);
  EXPECT_EQ(col2_opt->get().field_id(), original_last_id + 2);
}

TEST_F(UpdateSchemaTest, AddNestedStructColumn) {
  auto nested_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "field1", int32(), true, "First nested field"),
      SchemaField(101, "field2", string(), false, "Second nested field")});

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("complex_struct", nested_struct, "A complex struct column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("complex_struct"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  EXPECT_EQ(struct_field.name(), "complex_struct");
  EXPECT_TRUE(struct_field.type()->is_nested());
  EXPECT_TRUE(struct_field.optional());

  const auto& nested_type = checked_cast<const StructType&>(*struct_field.type());
  EXPECT_EQ(nested_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_type.GetFieldByName("field1"));
  ICEBERG_UNWRAP_OR_FAIL(auto field2_opt, nested_type.GetFieldByName("field2"));

  ASSERT_TRUE(field1_opt.has_value());
  ASSERT_TRUE(field2_opt.has_value());

  EXPECT_EQ(field1_opt->get().type(), int32());
  EXPECT_EQ(field2_opt->get().type(), string());
  EXPECT_TRUE(field1_opt->get().optional());
  EXPECT_FALSE(field2_opt->get().optional());
}

TEST_F(UpdateSchemaTest, CaseSensitiveColumnNames) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(true)
      .AddColumn("Column", int32(), "Uppercase column")
      .AddColumn("column", string(), "Lowercase column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto upper_opt, result.schema->FindFieldByName("Column", true));
  ICEBERG_UNWRAP_OR_FAIL(auto lower_opt, result.schema->FindFieldByName("column", true));

  ASSERT_TRUE(upper_opt.has_value());
  ASSERT_TRUE(lower_opt.has_value());

  EXPECT_EQ(upper_opt->get().type(), int32());
  EXPECT_EQ(lower_opt->get().type(), string());
}

TEST_F(UpdateSchemaTest, CaseInsensitiveDuplicateDetection) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("Column", int32(), "First column")
      .AddColumn("COLUMN", string(), "Duplicate column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Duplicate path found"));
}

TEST_F(UpdateSchemaTest, EmptyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto original_schema, table_->schema());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  EXPECT_EQ(*result.schema, *original_schema);
  EXPECT_EQ(result.new_last_column_id, table_->metadata()->last_column_id);
}

TEST_F(UpdateSchemaTest, CommitSuccess) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("committed_col", int64(), "A committed column");

  EXPECT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, reloaded->schema());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, schema->FindFieldByName("committed_col"));
  ASSERT_TRUE(field_opt.has_value());

  const auto& field = field_opt->get();
  EXPECT_EQ(field.name(), "committed_col");
  EXPECT_EQ(*field.type(), *int64());
  EXPECT_TRUE(field.optional());
  EXPECT_EQ(field.doc(), "A committed column");
}

TEST_F(UpdateSchemaTest, AddFieldsToMapAndList) {
  auto map_key_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(20, "address", string(), false),
                               SchemaField(21, "city", string(), false)});

  auto map_value_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(12, "lat", float32(), false),
                               SchemaField(13, "long", float32(), false)});

  auto map_type =
      std::make_shared<MapType>(SchemaField(10, "key", map_key_struct, false),
                                SchemaField(11, "value", map_value_struct, false));

  auto list_element_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(15, "x", int64(), false), SchemaField(16, "y", int64(), false)});

  auto list_type =
      std::make_shared<ListType>(SchemaField(14, "element", list_element_struct, true));

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("locations", map_type, "map of address to coordinate")
      .AddColumn("points", list_type, "2-D cartesian points");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("locations", "alt", float32(), "altitude")
      .AddColumn("points", "z", int64(), "z coordinate");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());
  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kMap);

  const auto& map = checked_cast<const MapType&>(*locations_field.type());
  const auto& value_struct = checked_cast<const StructType&>(*map.value().type());
  ICEBERG_UNWRAP_OR_FAIL(auto alt_opt, value_struct.GetFieldByName("alt"));
  ASSERT_TRUE(alt_opt.has_value());
  EXPECT_EQ(alt_opt->get().type(), float32());

  ICEBERG_UNWRAP_OR_FAIL(auto points_opt, result.schema->FindFieldByName("points"));
  ASSERT_TRUE(points_opt.has_value());
  const auto& points_field = points_opt->get();
  ASSERT_EQ(points_field.type()->type_id(), TypeId::kList);

  const auto& list = checked_cast<const ListType&>(*points_field.type());
  const auto& element_struct = checked_cast<const StructType&>(*list.element().type());
  ICEBERG_UNWRAP_OR_FAIL(auto z_opt, element_struct.GetFieldByName("z"));
  ASSERT_TRUE(z_opt.has_value());
  EXPECT_EQ(z_opt->get().type(), int64());
}

TEST_F(UpdateSchemaTest, AddNestedStructWithIdReassignment) {
  auto nested_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(1, "lat", int32(), false), SchemaField(2, "long", int32(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("location", nested_struct);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto location_opt, result.schema->FindFieldByName("location"));
  ASSERT_TRUE(location_opt.has_value());

  const auto& location_field = location_opt->get();
  ASSERT_TRUE(location_field.type()->is_nested());

  const auto& struct_type = checked_cast<const StructType&>(*location_field.type());
  ASSERT_EQ(struct_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());

  EXPECT_GT(lat_opt->get().field_id(), 1);
  EXPECT_GT(long_opt->get().field_id(), 1);
}

TEST_F(UpdateSchemaTest, AddNestedMapOfStructs) {
  auto key_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(20, "address", string(), false),
      SchemaField(21, "city", string(), false), SchemaField(22, "state", string(), false),
      SchemaField(23, "zip", int32(), false)});

  auto value_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(9, "lat", int32(), false), SchemaField(8, "long", int32(), true)});

  auto map_type = std::make_shared<MapType>(SchemaField(1, "key", key_struct, false),
                                            SchemaField(2, "value", value_struct, true));

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("locations", map_type);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());

  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kMap);

  const auto& map = checked_cast<const MapType&>(*locations_field.type());

  const auto& key_struct_type = checked_cast<const StructType&>(*map.key().type());
  EXPECT_EQ(key_struct_type.fields().size(), 4);

  const auto& value_struct_type = checked_cast<const StructType&>(*map.value().type());
  EXPECT_EQ(value_struct_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, value_struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, value_struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());
}

TEST_F(UpdateSchemaTest, AddNestedListOfStructs) {
  auto element_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(9, "lat", int32(), false), SchemaField(8, "long", int32(), true)});

  auto list_type =
      std::make_shared<ListType>(SchemaField(1, "element", element_struct, true));

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("locations", list_type);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());

  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kList);

  const auto& list = checked_cast<const ListType&>(*locations_field.type());
  const auto& element_struct_type =
      checked_cast<const StructType&>(*list.element().type());

  EXPECT_EQ(element_struct_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, element_struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, element_struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());
}

TEST_F(UpdateSchemaTest, AddFieldWithDotsInName) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "field.with.dots", int64(), "Field with dots in name");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = checked_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto dotted_field_opt,
                         nested_struct.GetFieldByName("field.with.dots"));
  ASSERT_TRUE(dotted_field_opt.has_value());
  EXPECT_EQ(dotted_field_opt->get().name(), "field.with.dots");
  EXPECT_EQ(dotted_field_opt->get().type(), int64());
}

TEST_F(UpdateSchemaTest, AddFieldToMapKeyFails) {
  auto key_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(20, "address", string(), false)});

  auto value_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(12, "lat", float32(), false)});

  auto map_type =
      std::make_shared<MapType>(SchemaField(10, "key", key_struct, false),
                                SchemaField(11, "value", value_struct, false));

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("locations", map_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("locations.key", "city", string(), "Should fail");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add fields to map keys"));
}

TEST_F(UpdateSchemaTest, DeleteColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("to_delete", string(), "A column to delete");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("to_delete");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("to_delete"));
  EXPECT_FALSE(field_opt.has_value());
}

TEST_F(UpdateSchemaTest, DeleteNestedColumn) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true),
                               SchemaField(101, "field2", string(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("struct_col.field1");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = checked_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_struct.GetFieldByName("field1"));
  ICEBERG_UNWRAP_OR_FAIL(auto field2_opt, nested_struct.GetFieldByName("field2"));

  EXPECT_FALSE(field1_opt.has_value());
  EXPECT_TRUE(field2_opt.has_value());
}

TEST_F(UpdateSchemaTest, DeleteMissingColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->DeleteColumn("non_existent");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete missing column"));
}

TEST_F(UpdateSchemaTest, DeleteThenAdd) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges().AddRequiredColumn("col", int32(),
                                                             "Required column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").AddColumn("col", string(), "Now optional string");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("col"));
  ASSERT_TRUE(field_opt.has_value());

  const auto& field = field_opt->get();
  EXPECT_EQ(field.type(), string());
  EXPECT_TRUE(field.optional());
}

TEST_F(UpdateSchemaTest, DeleteThenAddNested) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", boolean(), false)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("struct_col.field1")
      .AddColumn("struct_col", "field1", int32(), "Re-added field");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = checked_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_struct.GetFieldByName("field1"));
  ASSERT_TRUE(field1_opt.has_value());
  EXPECT_EQ(field1_opt->get().type(), int32());
}

TEST_F(UpdateSchemaTest, AddDeleteConflict) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col", int32()).DeleteColumn("new_col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete missing column"));
}

TEST_F(UpdateSchemaTest, DeleteColumnWithAdditionsFails) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "field2", string()).DeleteColumn("struct_col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete a column that has additions"));
}

TEST_F(UpdateSchemaTest, DeleteMapKeyFails) {
  auto map_type = std::make_shared<MapType>(SchemaField(10, "key", string(), false),
                                            SchemaField(11, "value", int32(), true));

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("map_col", map_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("map_col.key");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete map keys"));
}

TEST_F(UpdateSchemaTest, DeleteColumnCaseInsensitive) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("MyColumn", string(), "A column with mixed case");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->CaseSensitive(false).DeleteColumn("mycolumn");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt,
                         result.schema->FindFieldByName("MyColumn", false));
  EXPECT_FALSE(field_opt.has_value());
}

TEST_F(UpdateSchemaTest, RenameColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("old_name", string(), "A column to rename");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("old_name", "new_name");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto old_field_opt, result.schema->FindFieldByName("old_name"));
  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt, result.schema->FindFieldByName("new_name"));

  EXPECT_FALSE(old_field_opt.has_value());
  ASSERT_TRUE(new_field_opt.has_value());
  EXPECT_EQ(*new_field_opt->get().type(), *string());
}

TEST_F(UpdateSchemaTest, RenameNestedColumn) {
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true),
                               SchemaField(101, "field2", string(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("struct_col.field1", "renamed_field");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = checked_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_struct.GetFieldByName("field1"));
  ICEBERG_UNWRAP_OR_FAIL(auto renamed_opt, nested_struct.GetFieldByName("renamed_field"));

  EXPECT_FALSE(field1_opt.has_value());
  ASSERT_TRUE(renamed_opt.has_value());
  EXPECT_EQ(*renamed_opt->get().type(), *int32());
}

TEST_F(UpdateSchemaTest, RenameColumnWithDotsInName) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("simple_name", string());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("simple_name", "name.with.dots");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt,
                         result.schema->FindFieldByName("name.with.dots"));
  ASSERT_TRUE(new_field_opt.has_value());
  EXPECT_EQ(*new_field_opt->get().type(), *string());
}

TEST_F(UpdateSchemaTest, RenameMissingColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->RenameColumn("non_existent", "new_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename missing column"));
}

TEST_F(UpdateSchemaTest, RenameDeletedColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", string());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").RenameColumn("col", "new_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, RenameColumnCaseInsensitive) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("MyColumn", string(), "A column with mixed case");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->CaseSensitive(false).RenameColumn("mycolumn", "NewName");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto old_field_opt,
                         result.schema->FindFieldByName("MyColumn", false));
  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt,
                         result.schema->FindFieldByName("NewName", false));

  EXPECT_FALSE(old_field_opt.has_value());
  ASSERT_TRUE(new_field_opt.has_value());
}

TEST_F(UpdateSchemaTest, RenameThenDeleteOldNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("old_name", string());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("old_name", "new_name").DeleteColumn("old_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete a column that has updates"));
}

TEST_F(UpdateSchemaTest, RenameThenDeleteNewNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("old_name", string());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("old_name", "new_name").DeleteColumn("new_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete missing column"));
}

TEST_F(UpdateSchemaTest, RenameThenAddWithOldName) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("old_name", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("old_name", "new_name").AddColumn("old_name", string());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add column, name already exists"));
}

TEST_F(UpdateSchemaTest, AddThenRename) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("temp_name", string()).RenameColumn("temp_name", "final_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename missing column"));
}

TEST_F(UpdateSchemaTest, DeleteThenAddThenRename) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col")
      .AddColumn("col", string(), "New column with same name")
      .RenameColumn("col", "renamed_col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, MakeColumnOptional) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges().AddRequiredColumn("id", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MakeColumnOptional("id");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_TRUE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, RequireColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("id", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RequireColumn("id");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column nullability"));
  EXPECT_THAT(result, HasErrorMessage("optional -> required"));

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded2, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update2, reloaded2->NewUpdateSchema());
  update2->AllowIncompatibleChanges().RequireColumn("id");

  ICEBERG_UNWRAP_OR_FAIL(auto result2, update2->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result2.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_FALSE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, RequireColumnNoop) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges().AddRequiredColumn("id", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RequireColumn("id");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_FALSE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, MakeColumnOptionalNoop) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("id", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MakeColumnOptional("id");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_TRUE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, RequireColumnCaseInsensitive) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("ID", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->CaseSensitive(false).AllowIncompatibleChanges().RequireColumn("id");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("ID", false));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_FALSE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, MakeColumnOptionalMissingFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->MakeColumnOptional("non_existent");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update missing column"));
}

TEST_F(UpdateSchemaTest, RequireColumnMissingFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges().RequireColumn("non_existent");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update missing column"));
}

TEST_F(UpdateSchemaTest, MakeColumnOptionalDeletedFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges().AddRequiredColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").MakeColumnOptional("col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, RequireColumnDeletedFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").AllowIncompatibleChanges().RequireColumn("col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDoc) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col", int32(), "original doc");
  update->UpdateColumnDoc("col", "updated doc");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("col"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(field_opt->get().doc(), "updated doc");
}

TEST_F(UpdateSchemaTest, UpdateColumnDocMissingFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->UpdateColumnDoc("non_existent", "some doc");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update missing column"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDocDeletedFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32(), "original doc");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").UpdateColumnDoc("col", "new doc");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDocNoop) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32(), "same doc");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->UpdateColumnDoc("col", "same doc");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("col"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(field_opt->get().doc(), "same doc");
}

TEST_F(UpdateSchemaTest, UpdateColumnDocEmptyString) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col", int32(), "original doc");
  update->UpdateColumnDoc("col", "");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("col"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(field_opt->get().doc(), "");
}

TEST_F(UpdateSchemaTest, UpdateColumnIntToLong) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("id", int32(), "An integer ID");
  update->UpdateColumn("id", int64());

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(*field_opt->get().type(), *int64());
  EXPECT_EQ(field_opt->get().doc(), "An integer ID");  // Doc preserved
}

TEST_F(UpdateSchemaTest, UpdateColumnFloatToDouble) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("value", float32(), "A float value");
  update->UpdateColumn("value", float64());

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("value"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(*field_opt->get().type(), *float64());
}

TEST_F(UpdateSchemaTest, UpdateColumnSameType) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("id", int32());
  update->UpdateColumn("id", int32());

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(*field_opt->get().type(), *int32());
}

TEST_F(UpdateSchemaTest, UpdateColumnMissingFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->UpdateColumn("non_existent", int64());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update missing column"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDeletedFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").UpdateColumn("col", int64());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot update a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, UpdateColumnInvalidPromotionFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("id", int64());
  update->UpdateColumn("id", int32());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column type"));
}

TEST_F(UpdateSchemaTest, UpdateColumnInvalidPromotionDoubleToFloatFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("value", float64());
  update->UpdateColumn("value", float32());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column type"));
}

TEST_F(UpdateSchemaTest, UpdateColumnIncompatibleTypesFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("id", int32());
  update->UpdateColumn("id", string());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column type"));
}

TEST_F(UpdateSchemaTest, RenameAndUpdateColumnInSameTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("old_name", int32());
  update->UpdateColumn("old_name", int64());
  update->RenameColumn("old_name", "new_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename missing column"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDecimalPrecisionWidening) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  auto decimal_10_2 = decimal(10, 2);
  auto decimal_20_2 = decimal(20, 2);
  update->AddColumn("price", decimal_10_2);
  update->UpdateColumn("price", decimal_20_2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("price"));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(*field_opt->get().type(), *decimal_20_2);
}

TEST_F(UpdateSchemaTest, UpdateColumnDecimalDifferentScaleFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  auto decimal_10_2 = decimal(10, 2);
  auto decimal_10_3 = decimal(10, 3);
  update->AddColumn("price", decimal_10_2);
  update->UpdateColumn("price", decimal_10_3);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column type"));
}

TEST_F(UpdateSchemaTest, UpdateColumnDecimalPrecisionNarrowingFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  auto decimal_20_2 = decimal(20, 2);
  auto decimal_10_2 = decimal(10, 2);
  update->AddColumn("price", decimal_20_2);
  update->UpdateColumn("price", decimal_10_2);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot change column type"));
}

TEST_F(UpdateSchemaTest, UpdateTypePreservesOtherMetadata) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("value", int32(), "A counter value");

  update->UpdateColumn("value", int64());

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("value"));
  ASSERT_TRUE(field_opt.has_value());

  const auto& field = field_opt->get();
  EXPECT_EQ(*field.type(), *int64());         // Type updated
  EXPECT_EQ(field.doc(), "A counter value");  // Doc preserved
  EXPECT_TRUE(field.optional());              // Optional preserved
}

TEST_F(UpdateSchemaTest, UpdateDocPreservesOtherMetadata) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges().AddRequiredColumn("id", int64(), "old doc");

  update->UpdateColumnDoc("id", "new doc");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(field_opt.has_value());

  const auto& field = field_opt->get();
  EXPECT_EQ(field.doc(), "new doc");   // Doc updated
  EXPECT_EQ(*field.type(), *int64());  // Type preserved
  EXPECT_FALSE(field.optional());      // Optional preserved (still required)
}

TEST_F(UpdateSchemaTest, RenameDeleteConflict) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->RenameColumn("col", "new_name").DeleteColumn("col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete a column that has updates"));
}

TEST_F(UpdateSchemaTest, DeleteRenameConflict) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("col", int32());
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").RenameColumn("col", "new_name");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename a column that will be deleted"));
}

TEST_F(UpdateSchemaTest, CaseInsensitiveAddThenUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("Foo", int32(), "A column with uppercase name")
      .UpdateColumn("foo", int64());

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("Foo", false));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(*field_opt->get().type(), *int64());
}

TEST_F(UpdateSchemaTest, CaseInsensitiveAddThenUpdateDoc) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("Foo", int32(), "original doc")
      .UpdateColumnDoc("foo", "updated doc");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("Foo", false));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_EQ(field_opt->get().doc(), "updated doc");
}

TEST_F(UpdateSchemaTest, CaseInsensitiveAddThenMakeOptional) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AllowIncompatibleChanges()
      .AddRequiredColumn("Foo", int32(), "required column")
      .MakeColumnOptional("foo");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("Foo", false));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_TRUE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, CaseInsensitiveAddThenRequire) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AllowIncompatibleChanges()
      .AddColumn("Foo", int32(), "optional column")
      .RequireColumn("foo");  // Require using lowercase

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("Foo", false));
  ASSERT_TRUE(field_opt.has_value());
  EXPECT_FALSE(field_opt->get().optional());
}

TEST_F(UpdateSchemaTest, MixedChanges) {
  auto preferences_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(8, "feature1", boolean(), false),
                               SchemaField(9, "feature2", boolean(), true)});

  auto address_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(20, "address", string(), false),
      SchemaField(21, "city", string(), false), SchemaField(22, "state", string(), false),
      SchemaField(23, "zip", int32(), false)});

  auto coordinate_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(12, "lat", float32(), false),
                               SchemaField(13, "long", float32(), false)});

  auto locations_map =
      std::make_shared<MapType>(SchemaField(10, "key", address_struct, false),
                                SchemaField(11, "value", coordinate_struct, false));

  auto point_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(15, "x", int64(), false), SchemaField(16, "y", int64(), false)});

  auto points_list =
      std::make_shared<ListType>(SchemaField(14, "element", point_struct, true));

  auto doubles_list =
      std::make_shared<ListType>(SchemaField(17, "element", float64(), false));

  auto properties_map = std::make_shared<MapType>(
      SchemaField(18, "key", string(), true), SchemaField(19, "value", string(), true));

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges()
      .AddRequiredColumn("id", int32())
      .AddRequiredColumn("data", string())
      .AddColumn("preferences", preferences_struct, "struct of named boolean options")
      .AddRequiredColumn("locations", locations_map, "map of address to coordinate")
      .AddColumn("points", points_list, "2-D cartesian points")
      .AddRequiredColumn("doubles", doubles_list)
      .AddColumn("properties", properties_map, "string map of properties");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());

  update->AddColumn("toplevel", decimal(9, 2))
      .AddColumn("locations", "alt", float32())  // add to map value struct
      .AddColumn("points", "z", int64())         // add to list element struct
      .AddColumn("points", "t.t", int64(), "name with '.'")
      .RenameColumn("data", "json")
      .RenameColumn("preferences", "options")
      .RenameColumn("preferences.feature2", "newfeature")  // rename inside renamed column
      .RenameColumn("locations.lat", "latitude")
      .RenameColumn("points.x", "X")
      .RenameColumn("points.y", "y.y")  // field name with dots
      .UpdateColumn("id", int64())
      .UpdateColumnDoc("id", "unique id")
      .UpdateColumn("locations.lat", float64())  // use original name before rename
      .UpdateColumnDoc("locations.lat", "latitude")
      .DeleteColumn("locations.long")
      .DeleteColumn("properties")
      .MakeColumnOptional("points.x")
      .AllowIncompatibleChanges()
      .RequireColumn("data")
      .AddRequiredColumn("locations", "description", string(), "Location description");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  ICEBERG_UNWRAP_OR_FAIL(auto id_opt, result.schema->FindFieldByName("id"));
  ASSERT_TRUE(id_opt.has_value());
  EXPECT_EQ(*id_opt->get().type(), *int64());
  EXPECT_EQ(id_opt->get().doc(), "unique id");
  EXPECT_FALSE(id_opt->get().optional());  // was required

  ICEBERG_UNWRAP_OR_FAIL(auto data_opt, result.schema->FindFieldByName("data"));
  ICEBERG_UNWRAP_OR_FAIL(auto json_opt, result.schema->FindFieldByName("json"));
  EXPECT_FALSE(data_opt.has_value());
  ASSERT_TRUE(json_opt.has_value());
  EXPECT_EQ(*json_opt->get().type(), *string());
  EXPECT_FALSE(json_opt->get().optional());  // now required

  ICEBERG_UNWRAP_OR_FAIL(auto preferences_opt,
                         result.schema->FindFieldByName("preferences"));
  ICEBERG_UNWRAP_OR_FAIL(auto options_opt, result.schema->FindFieldByName("options"));
  EXPECT_FALSE(preferences_opt.has_value());
  ASSERT_TRUE(options_opt.has_value());
  EXPECT_EQ(options_opt->get().doc(), "struct of named boolean options");

  const auto& options_struct =
      checked_cast<const StructType&>(*options_opt->get().type());
  ICEBERG_UNWRAP_OR_FAIL(auto feature1_opt, options_struct.GetFieldByName("feature1"));
  ICEBERG_UNWRAP_OR_FAIL(auto feature2_opt, options_struct.GetFieldByName("feature2"));
  ICEBERG_UNWRAP_OR_FAIL(auto newfeature_opt,
                         options_struct.GetFieldByName("newfeature"));
  EXPECT_TRUE(feature1_opt.has_value());
  EXPECT_FALSE(feature2_opt.has_value());
  ASSERT_TRUE(newfeature_opt.has_value());
  EXPECT_EQ(*newfeature_opt->get().type(), *boolean());

  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());
  EXPECT_EQ(locations_opt->get().doc(), "map of address to coordinate");
  EXPECT_FALSE(locations_opt->get().optional());  // was required

  const auto& locations_map_type =
      checked_cast<const MapType&>(*locations_opt->get().type());
  const auto& coord_struct =
      checked_cast<const StructType&>(*locations_map_type.value().type());

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, coord_struct.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto latitude_opt, coord_struct.GetFieldByName("latitude"));
  EXPECT_FALSE(lat_opt.has_value());
  ASSERT_TRUE(latitude_opt.has_value());
  EXPECT_EQ(*latitude_opt->get().type(), *float64());
  EXPECT_EQ(latitude_opt->get().doc(), "latitude");

  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, coord_struct.GetFieldByName("long"));
  EXPECT_FALSE(long_opt.has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto alt_opt, coord_struct.GetFieldByName("alt"));
  ASSERT_TRUE(alt_opt.has_value());
  EXPECT_EQ(*alt_opt->get().type(), *float32());

  ICEBERG_UNWRAP_OR_FAIL(auto description_opt,
                         coord_struct.GetFieldByName("description"));
  ASSERT_TRUE(description_opt.has_value());
  EXPECT_EQ(*description_opt->get().type(), *string());
  EXPECT_EQ(description_opt->get().doc(), "Location description");
  EXPECT_FALSE(description_opt->get().optional());

  ICEBERG_UNWRAP_OR_FAIL(auto points_opt, result.schema->FindFieldByName("points"));
  ASSERT_TRUE(points_opt.has_value());
  EXPECT_EQ(points_opt->get().doc(), "2-D cartesian points");

  const auto& points_list_type = checked_cast<const ListType&>(*points_opt->get().type());
  const auto& point_elem_struct =
      checked_cast<const StructType&>(*points_list_type.element().type());

  ICEBERG_UNWRAP_OR_FAIL(auto x_opt, point_elem_struct.GetFieldByName("x"));
  ICEBERG_UNWRAP_OR_FAIL(auto X_opt, point_elem_struct.GetFieldByName("X"));
  EXPECT_FALSE(x_opt.has_value());
  ASSERT_TRUE(X_opt.has_value());
  EXPECT_EQ(*X_opt->get().type(), *int64());
  EXPECT_TRUE(X_opt->get().optional());  // made optional

  ICEBERG_UNWRAP_OR_FAIL(auto y_opt, point_elem_struct.GetFieldByName("y"));
  ICEBERG_UNWRAP_OR_FAIL(auto yy_opt, point_elem_struct.GetFieldByName("y.y"));
  EXPECT_FALSE(y_opt.has_value());
  ASSERT_TRUE(yy_opt.has_value());
  EXPECT_EQ(*yy_opt->get().type(), *int64());

  ICEBERG_UNWRAP_OR_FAIL(auto z_opt, point_elem_struct.GetFieldByName("z"));
  ASSERT_TRUE(z_opt.has_value());
  EXPECT_EQ(*z_opt->get().type(), *int64());

  ICEBERG_UNWRAP_OR_FAIL(auto tt_opt, point_elem_struct.GetFieldByName("t.t"));
  ASSERT_TRUE(tt_opt.has_value());
  EXPECT_EQ(*tt_opt->get().type(), *int64());
  EXPECT_EQ(tt_opt->get().doc(), "name with '.'");

  ICEBERG_UNWRAP_OR_FAIL(auto doubles_opt, result.schema->FindFieldByName("doubles"));
  ASSERT_TRUE(doubles_opt.has_value());
  EXPECT_EQ(doubles_opt->get().type()->type_id(), TypeId::kList);

  ICEBERG_UNWRAP_OR_FAIL(auto properties_opt,
                         result.schema->FindFieldByName("properties"));
  EXPECT_FALSE(properties_opt.has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto toplevel_opt, result.schema->FindFieldByName("toplevel"));
  ASSERT_TRUE(toplevel_opt.has_value());
  EXPECT_EQ(*toplevel_opt->get().type(), *decimal(9, 2));
  EXPECT_TRUE(toplevel_opt->get().optional());
}

// ============================================================================
// Move Operations Tests
// ============================================================================

TEST_F(UpdateSchemaTest, TestMultipleMoves) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  setup->AddColumn("w", int64());
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());

  update->MoveFirst("w").MoveFirst("z").MoveAfter("y", "w").MoveBefore("w", "x");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  EXPECT_EQ(fields[0].name(), "z");
  EXPECT_EQ(fields[1].name(), "y");

  int w_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "w") w_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GT(w_pos, 0);
  EXPECT_GT(x_pos, 0);
  EXPECT_LT(w_pos, x_pos);  // w should come before x
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelColumnFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->MoveFirst("y");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  EXPECT_EQ(fields[0].name(), "y");
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelColumnBeforeFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->MoveBefore("y", "x");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int y_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "y") y_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GE(y_pos, 0);
  EXPECT_GE(x_pos, 0);
  EXPECT_LT(y_pos, x_pos);
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelColumnAfterLast) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->MoveAfter("x", "z");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  EXPECT_EQ(fields[fields.size() - 1].name(), "x");
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelColumnAfter) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  setup->AddColumn("w", timestamp_tz());
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveAfter("w", "x");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int w_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "w") w_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GE(w_pos, 0);
  EXPECT_GE(x_pos, 0);
  EXPECT_EQ(w_pos, x_pos + 1);  // w should be immediately after x
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelColumnBefore) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  setup->AddColumn("w", timestamp_tz());
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("w", "z");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int w_pos = -1;
  int z_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "w") w_pos = i;
    if (fields[i].name() == "z") z_pos = i;
  }
  EXPECT_GE(w_pos, 0);
  EXPECT_GE(z_pos, 0);
  EXPECT_LT(w_pos, z_pos);
}

TEST_F(UpdateSchemaTest, TestMoveNestedFieldFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "a", int64(), true), SchemaField(101, "b", int64(), true)});
  setup->AddColumn("s", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveFirst("s.b");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto s_opt, result.schema->FindFieldByName("s"));
  ASSERT_TRUE(s_opt.has_value());
  const auto& s_struct = checked_cast<const StructType&>(*s_opt->get().type());

  const auto& fields = s_struct.fields();
  EXPECT_EQ(fields[0].name(), "b");
}

TEST_F(UpdateSchemaTest, TestMoveNestedFieldBeforeFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "a", int64(), true), SchemaField(101, "b", int64(), true)});
  setup->AddColumn("s", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("s.b", "s.a");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto s_opt, result.schema->FindFieldByName("s"));
  ASSERT_TRUE(s_opt.has_value());
  const auto& s_struct = checked_cast<const StructType&>(*s_opt->get().type());

  const auto& fields = s_struct.fields();
  int a_pos = -1;
  int b_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "a") a_pos = i;
    if (fields[i].name() == "b") b_pos = i;
  }
  EXPECT_GE(a_pos, 0);
  EXPECT_GE(b_pos, 0);
  EXPECT_LT(b_pos, a_pos);
}

TEST_F(UpdateSchemaTest, TestMoveNestedFieldAfterLast) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "a", int64(), true), SchemaField(101, "b", int64(), true)});
  setup->AddColumn("s", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveAfter("s.a", "s.b");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto s_opt, result.schema->FindFieldByName("s"));
  ASSERT_TRUE(s_opt.has_value());
  const auto& s_struct = checked_cast<const StructType&>(*s_opt->get().type());

  const auto& fields = s_struct.fields();
  EXPECT_EQ(fields[fields.size() - 1].name(), "a");
}

TEST_F(UpdateSchemaTest, TestMoveNestedFieldAfter) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "a", int64(), true), SchemaField(101, "b", int64(), true),
      SchemaField(102, "c", timestamp_tz(), false)});
  setup->AddColumn("s", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveAfter("s.c", "s.a");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto s_opt, result.schema->FindFieldByName("s"));
  ASSERT_TRUE(s_opt.has_value());
  const auto& s_struct = checked_cast<const StructType&>(*s_opt->get().type());

  const auto& fields = s_struct.fields();
  int a_pos = -1;
  int c_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "a") a_pos = i;
    if (fields[i].name() == "c") c_pos = i;
  }
  EXPECT_GE(a_pos, 0);
  EXPECT_GE(c_pos, 0);
  EXPECT_EQ(c_pos, a_pos + 1);
}

TEST_F(UpdateSchemaTest, TestMoveNestedFieldBefore) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(102, "c", timestamp_tz(), false), SchemaField(100, "a", int64(), true),
      SchemaField(101, "b", int64(), true)});
  setup->AddColumn("s", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("s.c", "s.b");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto s_opt, result.schema->FindFieldByName("s"));
  ASSERT_TRUE(s_opt.has_value());
  const auto& s_struct = checked_cast<const StructType&>(*s_opt->get().type());

  const auto& fields = s_struct.fields();
  int b_pos = -1;
  int c_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "b") b_pos = i;
    if (fields[i].name() == "c") c_pos = i;
  }
  EXPECT_GE(b_pos, 0);
  EXPECT_GE(c_pos, 0);
  EXPECT_LT(c_pos, b_pos);
}

TEST_F(UpdateSchemaTest, TestMoveListElementField) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto elem_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "a", int64(), true), SchemaField(101, "b", int64(), true)});
  auto list_type =
      std::make_shared<ListType>(SchemaField(99, "element", elem_struct, false));
  setup->AddColumn("list", list_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveAfter("list.a", "list.b");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto list_opt, result.schema->FindFieldByName("list"));
  ASSERT_TRUE(list_opt.has_value());
  const auto& list_type_result = checked_cast<const ListType&>(*list_opt->get().type());
  const auto& elem_struct_result =
      checked_cast<const StructType&>(*list_type_result.element().type());

  const auto& fields = elem_struct_result.fields();
  int a_pos = -1;
  int b_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "a") a_pos = i;
    if (fields[i].name() == "b") b_pos = i;
  }
  EXPECT_GE(a_pos, 0);
  EXPECT_GE(b_pos, 0);
  EXPECT_GT(a_pos, b_pos);
}

TEST_F(UpdateSchemaTest, TestMoveMapValueStructField) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto value_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "lat", float64(), true),
                               SchemaField(101, "long", float64(), true)});
  auto map_type =
      std::make_shared<MapType>(SchemaField(98, "key", string(), false),
                                SchemaField(99, "value", value_struct, false));
  setup->AddColumn("locations", map_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveAfter("locations.lat", "locations.long");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto locs_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locs_opt.has_value());
  const auto& locs_map = checked_cast<const MapType&>(*locs_opt->get().type());
  const auto& value_struct_result =
      checked_cast<const StructType&>(*locs_map.value().type());

  const auto& fields = value_struct_result.fields();
  int lat_pos = -1;
  int long_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "lat") lat_pos = i;
    if (fields[i].name() == "long") long_pos = i;
  }
  EXPECT_GE(lat_pos, 0);
  EXPECT_GE(long_pos, 0);
  EXPECT_GT(lat_pos, long_pos);
}

TEST_F(UpdateSchemaTest, TestMoveAddedTopLevelColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("ts", timestamp_tz()).MoveAfter("ts", "x");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int ts_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "ts") ts_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GE(ts_pos, 0);
  EXPECT_GE(x_pos, 0);
  EXPECT_EQ(ts_pos, x_pos + 1);
}

TEST_F(UpdateSchemaTest, TestMoveAddedTopLevelColumnAfterAddedColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("ts", timestamp_tz())
      .AddColumn("count", int64())
      .MoveAfter("ts", "x")
      .MoveAfter("count", "ts");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int x_pos = -1;
  int ts_pos = -1;
  int count_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "x") x_pos = i;
    if (fields[i].name() == "ts") ts_pos = i;
    if (fields[i].name() == "count") count_pos = i;
  }
  EXPECT_GE(x_pos, 0);
  EXPECT_GE(ts_pos, 0);
  EXPECT_GE(count_pos, 0);
  EXPECT_EQ(ts_pos, x_pos + 1);
  EXPECT_EQ(count_pos, ts_pos + 1);
}

TEST_F(UpdateSchemaTest, TestMoveAddedNestedStructField) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true),
                               SchemaField(101, "feature2", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("preferences", "ts", timestamp_tz())
      .MoveBefore("preferences.ts", "preferences.feature1");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[0].name(), "ts");
}

TEST_F(UpdateSchemaTest, TestMoveAddedNestedStructFieldBeforeAddedColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true),
                               SchemaField(101, "feature2", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("preferences", "ts", timestamp_tz())
      .AddColumn("preferences", "size", int64())
      .MoveBefore("preferences.ts", "preferences.feature1")
      .MoveBefore("preferences.size", "preferences.ts");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[0].name(), "size");
  EXPECT_EQ(fields[1].name(), "ts");
}

TEST_F(UpdateSchemaTest, TestMoveSelfReferenceFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update1, table_->NewUpdateSchema());
  update1->MoveBefore("x", "x");
  auto result1 = update1->Apply();
  EXPECT_THAT(result1, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result1, HasErrorMessage("Cannot move x before itself"));

  ICEBERG_UNWRAP_OR_FAIL(auto update2, table_->NewUpdateSchema());
  update2->MoveAfter("x", "x");
  auto result2 = update2->Apply();
  EXPECT_THAT(result2, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result2, HasErrorMessage("Cannot move x after itself"));
}

TEST_F(UpdateSchemaTest, TestMoveMissingColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update1, table_->NewUpdateSchema());
  update1->MoveFirst("items");
  auto result1 = update1->Apply();
  EXPECT_THAT(result1, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result1, HasErrorMessage("Cannot move missing column: items"));

  ICEBERG_UNWRAP_OR_FAIL(auto update2, table_->NewUpdateSchema());
  update2->MoveBefore("items", "x");
  auto result2 = update2->Apply();
  EXPECT_THAT(result2, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result2, HasErrorMessage("Cannot move missing column: items"));

  ICEBERG_UNWRAP_OR_FAIL(auto update3, table_->NewUpdateSchema());
  update3->MoveAfter("items", "y");
  auto result3 = update3->Apply();
  EXPECT_THAT(result3, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result3, HasErrorMessage("Cannot move missing column: items"));
}

TEST_F(UpdateSchemaTest, TestMoveBeforeAddFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->MoveBefore("ts", "x").AddColumn("ts", timestamp_tz());
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot move missing column: ts"));
}

TEST_F(UpdateSchemaTest, TestMoveMissingReferenceColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update1, table_->NewUpdateSchema());
  update1->MoveBefore("x", "items");
  auto result1 = update1->Apply();
  EXPECT_THAT(result1, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result1, HasErrorMessage("Cannot move x before missing column: items"));

  ICEBERG_UNWRAP_OR_FAIL(auto update2, table_->NewUpdateSchema());
  update2->MoveAfter("y", "items");
  auto result2 = update2->Apply();
  EXPECT_THAT(result2, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result2, HasErrorMessage("Cannot move y after missing column: items"));
}

TEST_F(UpdateSchemaTest, TestMovePrimitiveMapKeyFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto map_type = std::make_shared<MapType>(SchemaField(98, "key", string(), false),
                                            SchemaField(99, "value", string(), false));

  setup->AllowIncompatibleChanges().AddRequiredColumn("properties", map_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("properties.key", "properties.value");
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot move fields in non-struct type"));
}

TEST_F(UpdateSchemaTest, TestMovePrimitiveMapValueFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto map_type = std::make_shared<MapType>(SchemaField(98, "key", string(), false),
                                            SchemaField(99, "value", string(), false));

  setup->AllowIncompatibleChanges().AddRequiredColumn("properties", map_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("properties.value", "properties.key");
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot move fields in non-struct type"));
}

TEST_F(UpdateSchemaTest, TestMovePrimitiveListElementFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto list_type =
      std::make_shared<ListType>(SchemaField(98, "element", float64(), false));

  setup->AllowIncompatibleChanges().AddRequiredColumn("doubles", list_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("doubles.element", "doubles");
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot move fields in non-struct type"));
}

TEST_F(UpdateSchemaTest, TestMoveTopLevelBetweenStructsFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("x", "preferences.feature1");
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot move field x to a different struct"));
}

TEST_F(UpdateSchemaTest, TestMoveBetweenStructsFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct1 = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "a", int64(), true)});
  auto struct2 = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(101, "feature1", string(), true)});
  setup->AddColumn("points", struct1);
  setup->AddColumn("preferences", struct2);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->MoveBefore("points.a", "preferences.feature1");
  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot move field points.a to a different struct"));
}

TEST_F(UpdateSchemaTest, TestMoveTopDeletedColumnAfterAnotherColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("z")
      .AddRequiredColumn("z", int32())
      .MoveAfter("z", "y");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int z_pos = -1;
  int y_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "z") z_pos = i;
    if (fields[i].name() == "y") y_pos = i;
  }
  EXPECT_GE(z_pos, 0);
  EXPECT_GE(y_pos, 0);
  EXPECT_GT(z_pos, y_pos);
}

TEST_F(UpdateSchemaTest, TestMoveTopDeletedColumnBeforeAnotherColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("z")
      .AddRequiredColumn("z", int32())
      .MoveBefore("z", "x");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int z_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "z") z_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GE(z_pos, 0);
  EXPECT_GE(x_pos, 0);
  EXPECT_LT(z_pos, x_pos);
}

TEST_F(UpdateSchemaTest, TestMoveTopDeletedColumnToFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("z")
      .AddRequiredColumn("z", int32())
      .MoveFirst("z");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  EXPECT_EQ(fields[0].name(), "z");
}

TEST_F(UpdateSchemaTest, TestMoveDeletedNestedStructFieldAfterAnotherColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true),
                               SchemaField(101, "feature2", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("preferences.feature1")
      .AddRequiredColumn("preferences", "feature1", boolean())
      .MoveAfter("preferences.feature1", "preferences.feature2");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[fields.size() - 1].name(), "feature1");
}

TEST_F(UpdateSchemaTest, TestMoveDeletedNestedStructFieldBeforeAnotherColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true),
                               SchemaField(101, "feature2", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("preferences.feature2")
      .AddColumn("preferences", "feature2", boolean())
      .MoveBefore("preferences.feature2", "preferences.feature1");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[0].name(), "feature2");
}

TEST_F(UpdateSchemaTest, TestMoveDeletedNestedStructFieldToFirst) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "feature1", string(), true),
                               SchemaField(101, "feature2", string(), true)});
  setup->AddColumn("preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AllowIncompatibleChanges()
      .DeleteColumn("preferences.feature2")
      .AddColumn("preferences", "feature2", boolean())
      .MoveFirst("preferences.feature2");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[0].name(), "feature2");
}

TEST_F(UpdateSchemaTest, TestCaseInsensitiveAddTopLevelAndMove) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false).AddColumn("TS", timestamp_tz()).MoveAfter("ts", "X");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int ts_pos = -1;
  int x_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "TS") ts_pos = i;
    if (fields[i].name() == "x") x_pos = i;
  }
  EXPECT_GE(ts_pos, 0);
  EXPECT_GE(x_pos, 0);
  EXPECT_EQ(ts_pos, x_pos + 1);
}

TEST_F(UpdateSchemaTest, TestCaseInsensitiveAddNestedAndMove) {
  ICEBERG_UNWRAP_OR_FAIL(auto setup, table_->NewUpdateSchema());
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "Feature1", string(), true)});
  setup->AddColumn("Preferences", struct_type);
  EXPECT_THAT(setup->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("Preferences", "TS", timestamp_tz())
      .MoveBefore("preferences.ts", "PREFERENCES.Feature1");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto prefs_opt, result.schema->FindFieldByName("Preferences"));
  ASSERT_TRUE(prefs_opt.has_value());
  const auto& prefs_struct = checked_cast<const StructType&>(*prefs_opt->get().type());

  const auto& fields = prefs_struct.fields();
  EXPECT_EQ(fields[0].name(), "TS");
}

TEST_F(UpdateSchemaTest, TestCaseInsensitiveMoveAfterNewlyAddedField) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("TS", timestamp_tz())
      .AddColumn("Count", int64())
      .MoveAfter("ts", "X")
      .MoveAfter("count", "TS");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  const auto& fields = result.schema->fields();
  int x_pos = -1;
  int ts_pos = -1;
  int count_pos = -1;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name() == "x") x_pos = i;
    if (fields[i].name() == "TS") ts_pos = i;
    if (fields[i].name() == "Count") count_pos = i;
  }
  EXPECT_GE(x_pos, 0);
  EXPECT_GE(ts_pos, 0);
  EXPECT_GE(count_pos, 0);
  EXPECT_EQ(ts_pos, x_pos + 1);
  EXPECT_EQ(count_pos, ts_pos + 1);
}

}  // namespace iceberg
