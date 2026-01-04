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

namespace iceberg {

class UpdateSchemaTest : public UpdateTestBase {};

// Test adding a simple optional column
TEST_F(UpdateSchemaTest, AddOptionalColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col", int32(), "A new integer column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Verify the new column was added
  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt, result.schema->FindFieldByName("new_col"));
  ASSERT_TRUE(new_field_opt.has_value());

  const auto& new_field = new_field_opt->get();
  EXPECT_EQ(new_field.name(), "new_col");
  EXPECT_EQ(new_field.type(), int32());
  EXPECT_TRUE(new_field.optional());
  EXPECT_EQ(new_field.doc(), "A new integer column");
}

// Test adding a required column (should fail without AllowIncompatibleChanges)
TEST_F(UpdateSchemaTest, AddRequiredColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddRequiredColumn("required_col", string(), "A required string column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Incompatible change"));
}

// Test adding a required column with AllowIncompatibleChanges
TEST_F(UpdateSchemaTest, AddRequiredColumnWithAllowIncompatible) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AllowIncompatibleChanges().AddRequiredColumn("required_col", string(),
                                                       "A required string column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Verify the new column was added
  ICEBERG_UNWRAP_OR_FAIL(auto new_field_opt,
                         result.schema->FindFieldByName("required_col"));
  ASSERT_TRUE(new_field_opt.has_value());

  const auto& new_field = new_field_opt->get();
  EXPECT_EQ(new_field.name(), "required_col");
  EXPECT_EQ(new_field.type(), string());
  EXPECT_FALSE(new_field.optional());
  EXPECT_EQ(new_field.doc(), "A required string column");
}

// Test adding multiple columns
TEST_F(UpdateSchemaTest, AddMultipleColumns) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col1", int32(), "First column")
      .AddColumn("col2", string(), "Second column")
      .AddColumn("col3", boolean(), "Third column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Verify all columns were added
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

// Test adding column with dot in name should fail for top-level
TEST_F(UpdateSchemaTest, AddColumnWithDotInNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("col.with.dots", int32(), "Column with dots");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add column with ambiguous name"));
}

// Test adding column to nested struct
TEST_F(UpdateSchemaTest, AddColumnToNestedStruct) {
  // First, add a struct column to the table
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "nested_field", int32(), true, "Nested field")});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type, "A struct column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Reload table and add column to the nested struct
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "new_nested_field", string(), "New nested field");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Verify the nested field was added
  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  ASSERT_TRUE(struct_field.type()->is_nested());

  const auto& nested_struct = static_cast<const StructType&>(*struct_field.type());
  ICEBERG_UNWRAP_OR_FAIL(auto nested_field_opt,
                         nested_struct.GetFieldByName("new_nested_field"));
  ASSERT_TRUE(nested_field_opt.has_value());

  const auto& nested_field = nested_field_opt->get();
  EXPECT_EQ(nested_field.name(), "new_nested_field");
  EXPECT_EQ(nested_field.type(), string());
}

// Test adding column to non-existent parent fails
TEST_F(UpdateSchemaTest, AddColumnToNonExistentParentFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("non_existent_parent", "new_field", int32(), "New field");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot find parent struct"));
}

// Test adding column to non-struct parent fails
TEST_F(UpdateSchemaTest, AddColumnToNonStructParentFails) {
  // First, add a primitive column
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("primitive_col", int32(), "A primitive column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Try to add column to the primitive column (should fail)
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("primitive_col", "nested_field", string(), "Should fail");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add to non-struct column"));
}

// Test adding duplicate column name fails
TEST_F(UpdateSchemaTest, AddDuplicateColumnNameFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("duplicate_col", int32(), "First column")
      .AddColumn("duplicate_col", string(), "Duplicate column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Duplicate path found"));
}

// Test column ID assignment
TEST_F(UpdateSchemaTest, ColumnIdAssignment) {
  ICEBERG_UNWRAP_OR_FAIL(auto original_schema, table_->schema());
  int32_t original_last_id = table_->metadata()->last_column_id;

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col1", int32(), "First new column")
      .AddColumn("new_col2", string(), "Second new column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify new last column ID is incremented
  EXPECT_EQ(result.new_last_column_id, original_last_id + 2);

  // Verify new columns have sequential IDs
  ICEBERG_UNWRAP_OR_FAIL(auto col1_opt, result.schema->FindFieldByName("new_col1"));
  ICEBERG_UNWRAP_OR_FAIL(auto col2_opt, result.schema->FindFieldByName("new_col2"));

  ASSERT_TRUE(col1_opt.has_value());
  ASSERT_TRUE(col2_opt.has_value());

  EXPECT_EQ(col1_opt->get().field_id(), original_last_id + 1);
  EXPECT_EQ(col2_opt->get().field_id(), original_last_id + 2);
}

// Test adding nested struct with multiple fields
TEST_F(UpdateSchemaTest, AddNestedStructColumn) {
  auto nested_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(100, "field1", int32(), true, "First nested field"),
      SchemaField(101, "field2", string(), false, "Second nested field")});

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("complex_struct", nested_struct, "A complex struct column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Verify the struct column was added
  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("complex_struct"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  EXPECT_EQ(struct_field.name(), "complex_struct");
  EXPECT_TRUE(struct_field.type()->is_nested());
  EXPECT_TRUE(struct_field.optional());

  // Verify nested fields exist
  const auto& nested_type = static_cast<const StructType&>(*struct_field.type());
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

// Test case sensitivity
TEST_F(UpdateSchemaTest, CaseSensitiveColumnNames) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(true)
      .AddColumn("Column", int32(), "Uppercase column")
      .AddColumn("column", string(), "Lowercase column");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  ASSERT_TRUE(result.schema != nullptr);

  // Both columns should exist with case-sensitive search
  ICEBERG_UNWRAP_OR_FAIL(auto upper_opt, result.schema->FindFieldByName("Column", true));
  ICEBERG_UNWRAP_OR_FAIL(auto lower_opt, result.schema->FindFieldByName("column", true));

  ASSERT_TRUE(upper_opt.has_value());
  ASSERT_TRUE(lower_opt.has_value());

  EXPECT_EQ(upper_opt->get().type(), int32());
  EXPECT_EQ(lower_opt->get().type(), string());
}

// Test case insensitive duplicate detection
TEST_F(UpdateSchemaTest, CaseInsensitiveDuplicateDetection) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->CaseSensitive(false)
      .AddColumn("Column", int32(), "First column")
      .AddColumn("COLUMN", string(), "Duplicate column");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Duplicate path found"));
}

// Test empty update
TEST_F(UpdateSchemaTest, EmptyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto original_schema, table_->schema());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Schema should be unchanged
  EXPECT_EQ(*result.schema, *original_schema);
  EXPECT_EQ(result.new_last_column_id, table_->metadata()->last_column_id);
}

// Test commit success
TEST_F(UpdateSchemaTest, CommitSuccess) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("committed_col", int64(), "A committed column");

  EXPECT_THAT(update->Commit(), IsOk());

  // Reload table and verify column exists
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

// Test adding fields to map value and list element structs
TEST_F(UpdateSchemaTest, AddFieldsToMapAndList) {
  // Create a schema with map and list of structs
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

  // Reload and add fields to nested structs
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update
      ->AddColumn("locations", "alt", float32(), "altitude")  // add to map value
      .AddColumn("points", "z", int64(), "z coordinate");     // add to list element

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify map value has new field
  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());
  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kMap);

  const auto& map = static_cast<const MapType&>(*locations_field.type());
  const auto& value_struct = static_cast<const StructType&>(*map.value().type());
  ICEBERG_UNWRAP_OR_FAIL(auto alt_opt, value_struct.GetFieldByName("alt"));
  ASSERT_TRUE(alt_opt.has_value());
  EXPECT_EQ(alt_opt->get().type(), float32());

  // Verify list element has new field
  ICEBERG_UNWRAP_OR_FAIL(auto points_opt, result.schema->FindFieldByName("points"));
  ASSERT_TRUE(points_opt.has_value());
  const auto& points_field = points_opt->get();
  ASSERT_EQ(points_field.type()->type_id(), TypeId::kList);

  const auto& list = static_cast<const ListType&>(*points_field.type());
  const auto& element_struct = static_cast<const StructType&>(*list.element().type());
  ICEBERG_UNWRAP_OR_FAIL(auto z_opt, element_struct.GetFieldByName("z"));
  ASSERT_TRUE(z_opt.has_value());
  EXPECT_EQ(z_opt->get().type(), int64());
}

// Test adding nested struct with ID reassignment
TEST_F(UpdateSchemaTest, AddNestedStructWithIdReassignment) {
  // Create a struct with conflicting IDs (will be reassigned)
  auto nested_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(1, "lat", int32(), false),    // ID 1 conflicts with existing schema
      SchemaField(2, "long", int32(), true)});  // ID 2 conflicts with existing schema

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("location", nested_struct);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the struct was added with reassigned IDs
  ICEBERG_UNWRAP_OR_FAIL(auto location_opt, result.schema->FindFieldByName("location"));
  ASSERT_TRUE(location_opt.has_value());

  const auto& location_field = location_opt->get();
  ASSERT_TRUE(location_field.type()->is_nested());

  const auto& struct_type = static_cast<const StructType&>(*location_field.type());
  ASSERT_EQ(struct_type.fields().size(), 2);

  // IDs should be reassigned to avoid conflicts
  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());

  // IDs should be > 1 (reassigned)
  EXPECT_GT(lat_opt->get().field_id(), 1);
  EXPECT_GT(long_opt->get().field_id(), 1);
}

// Test adding nested map of structs with ID reassignment
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

  // Verify the map was added with reassigned IDs
  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());

  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kMap);

  const auto& map = static_cast<const MapType&>(*locations_field.type());

  // Verify key struct fields
  const auto& key_struct_type = static_cast<const StructType&>(*map.key().type());
  EXPECT_EQ(key_struct_type.fields().size(), 4);

  // Verify value struct fields
  const auto& value_struct_type = static_cast<const StructType&>(*map.value().type());
  EXPECT_EQ(value_struct_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, value_struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, value_struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());
}

// Test adding nested list of structs with ID reassignment
TEST_F(UpdateSchemaTest, AddNestedListOfStructs) {
  auto element_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(9, "lat", int32(), false), SchemaField(8, "long", int32(), true)});

  auto list_type =
      std::make_shared<ListType>(SchemaField(1, "element", element_struct, true));

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("locations", list_type);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the list was added with reassigned IDs
  ICEBERG_UNWRAP_OR_FAIL(auto locations_opt, result.schema->FindFieldByName("locations"));
  ASSERT_TRUE(locations_opt.has_value());

  const auto& locations_field = locations_opt->get();
  ASSERT_EQ(locations_field.type()->type_id(), TypeId::kList);

  const auto& list = static_cast<const ListType&>(*locations_field.type());
  const auto& element_struct_type =
      static_cast<const StructType&>(*list.element().type());

  EXPECT_EQ(element_struct_type.fields().size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto lat_opt, element_struct_type.GetFieldByName("lat"));
  ICEBERG_UNWRAP_OR_FAIL(auto long_opt, element_struct_type.GetFieldByName("long"));

  ASSERT_TRUE(lat_opt.has_value());
  ASSERT_TRUE(long_opt.has_value());
}

// Test adding field with dots in name to nested struct
TEST_F(UpdateSchemaTest, AddFieldWithDotsInName) {
  // First add a struct column
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Add a field with dots in its name to the nested struct
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "field.with.dots", int64(), "Field with dots in name");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the field with dots was added
  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = static_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto dotted_field_opt,
                         nested_struct.GetFieldByName("field.with.dots"));
  ASSERT_TRUE(dotted_field_opt.has_value());
  EXPECT_EQ(dotted_field_opt->get().name(), "field.with.dots");
  EXPECT_EQ(dotted_field_opt->get().type(), int64());
}

// Test adding field to map key should fail
TEST_F(UpdateSchemaTest, AddFieldToMapKeyFails) {
  // Create a map with struct key
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

  // Try to add field to map key (should fail)
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("locations.key", "city", string(), "Should fail");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add fields to map keys"));
}

// Test deleting a column
TEST_F(UpdateSchemaTest, DeleteColumn) {
  // First add a column
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("to_delete", string(), "A column to delete");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Delete the column
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("to_delete");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the column was deleted
  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("to_delete"));
  EXPECT_FALSE(field_opt.has_value());
}

// Test deleting a nested column
TEST_F(UpdateSchemaTest, DeleteNestedColumn) {
  // First add a struct with nested fields
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true),
                               SchemaField(101, "field2", string(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Delete one of the nested fields
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("struct_col.field1");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify field1 was deleted but field2 still exists
  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = static_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_struct.GetFieldByName("field1"));
  ICEBERG_UNWRAP_OR_FAIL(auto field2_opt, nested_struct.GetFieldByName("field2"));

  EXPECT_FALSE(field1_opt.has_value());
  EXPECT_TRUE(field2_opt.has_value());
}

// Test deleting missing column fails
TEST_F(UpdateSchemaTest, DeleteMissingColumnFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->DeleteColumn("non_existent");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete missing column"));
}

// Test delete then add same column
TEST_F(UpdateSchemaTest, DeleteThenAdd) {
  // First add a required column
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AllowIncompatibleChanges().AddRequiredColumn("col", int32(),
                                                             "Required column");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Delete then add with different type and optional
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("col").AddColumn("col", string(), "Now optional string");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the column was re-added with new properties
  ICEBERG_UNWRAP_OR_FAIL(auto field_opt, result.schema->FindFieldByName("col"));
  ASSERT_TRUE(field_opt.has_value());

  const auto& field = field_opt->get();
  EXPECT_EQ(field.type(), string());
  EXPECT_TRUE(field.optional());
}

// Test delete then add nested field
TEST_F(UpdateSchemaTest, DeleteThenAddNested) {
  // First add a struct with a field
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", boolean(), false)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Delete then re-add the nested field with different type
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("struct_col.field1")
      .AddColumn("struct_col", "field1", int32(), "Re-added field");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the field was re-added
  ICEBERG_UNWRAP_OR_FAIL(auto struct_field_opt,
                         result.schema->FindFieldByName("struct_col"));
  ASSERT_TRUE(struct_field_opt.has_value());

  const auto& struct_field = struct_field_opt->get();
  const auto& nested_struct = static_cast<const StructType&>(*struct_field.type());

  ICEBERG_UNWRAP_OR_FAIL(auto field1_opt, nested_struct.GetFieldByName("field1"));
  ASSERT_TRUE(field1_opt.has_value());
  EXPECT_EQ(field1_opt->get().type(), int32());
}

// Test add-delete conflict
TEST_F(UpdateSchemaTest, AddDeleteConflict) {
  // Try to delete a newly added column (should fail - column doesn't exist in schema)
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("new_col", int32()).DeleteColumn("new_col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete missing column"));
}

// Test delete column that has additions fails
TEST_F(UpdateSchemaTest, DeleteColumnWithAdditionsFails) {
  // First add a struct
  auto struct_type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(100, "field1", int32(), true)});

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("struct_col", struct_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Try to add a field to the struct and delete it in the same update
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->AddColumn("struct_col", "field2", string()).DeleteColumn("struct_col");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete a column that has additions"));
}

// Test delete map key fails
TEST_F(UpdateSchemaTest, DeleteMapKeyFails) {
  // Create a map
  auto map_type = std::make_shared<MapType>(SchemaField(10, "key", string(), false),
                                            SchemaField(11, "value", int32(), true));

  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("map_col", map_type);
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Try to delete the map key (should fail in Apply)
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->DeleteColumn("map_col.key");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete map keys"));
}

// Test case insensitive delete
TEST_F(UpdateSchemaTest, DeleteColumnCaseInsensitive) {
  // First add a column
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateSchema());
  setup_update->AddColumn("MyColumn", string(), "A column with mixed case");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Delete using different case
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSchema());
  update->CaseSensitive(false).DeleteColumn("mycolumn");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());

  // Verify the column was deleted
  ICEBERG_UNWRAP_OR_FAIL(auto field_opt,
                         result.schema->FindFieldByName("MyColumn", false));
  EXPECT_FALSE(field_opt.has_value());
}

}  // namespace iceberg
