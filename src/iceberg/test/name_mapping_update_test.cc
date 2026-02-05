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

#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/json_serde_internal.h"
#include "iceberg/name_mapping.h"
#include "iceberg/schema.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/type.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"

namespace iceberg {

class UpdateMappingTest : public UpdateTestBase {};

TEST_F(UpdateMappingTest, AddColumnMappingUpdate) {
  // Set initial name mapping to match current schema (x, y, z)
  ICEBERG_UNWRAP_OR_FAIL(auto schema, table_->metadata()->Schema());
  auto initial_mapping = CreateMapping(*schema);
  ASSERT_TRUE(initial_mapping.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto mapping_json, ToJsonString(ToJson(**initial_mapping)));

  ICEBERG_UNWRAP_OR_FAIL(auto props_update, table_->NewUpdateProperties());
  props_update->Set(std::string(TableProperties::kDefaultNameMapping),
                    std::move(mapping_json));
  EXPECT_THAT(props_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  // Add column ts
  ICEBERG_UNWRAP_OR_FAIL(auto schema_update, reloaded->NewUpdateSchema());
  schema_update->AddColumn("ts", timestamp_tz(), "Timestamp");
  EXPECT_THAT(schema_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, catalog_->LoadTable(table_ident_));
  auto updated_mapping_str = updated_table->metadata()->properties.configs().at(
      std::string(TableProperties::kDefaultNameMapping));
  ICEBERG_UNWRAP_OR_FAIL(auto json, FromJsonString(updated_mapping_str));
  ICEBERG_UNWRAP_OR_FAIL(auto updated_mapping, NameMappingFromJson(json));

  auto expected = MappedFields::Make({
      MappedField{.names = {"x"}, .field_id = 1},
      MappedField{.names = {"y"}, .field_id = 2},
      MappedField{.names = {"z"}, .field_id = 3},
      MappedField{.names = {"ts"}, .field_id = 4},
  });
  EXPECT_EQ(updated_mapping->AsMappedFields(), *expected);
}

TEST_F(UpdateMappingTest, AddNestedColumnMappingUpdate) {
  // Set initial name mapping (schema has x, y, z)
  ICEBERG_UNWRAP_OR_FAIL(auto schema, table_->metadata()->Schema());
  auto initial_mapping = CreateMapping(*schema);
  ASSERT_TRUE(initial_mapping.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto mapping_json, ToJsonString(ToJson(**initial_mapping)));

  ICEBERG_UNWRAP_OR_FAIL(auto props_update, table_->NewUpdateProperties());
  props_update->Set(std::string(TableProperties::kDefaultNameMapping),
                    std::move(mapping_json));
  EXPECT_THAT(props_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  // Add point struct with x, y - mapping updated automatically on commit
  auto point_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(4, "x", float64()),
      SchemaField::MakeRequired(5, "y", float64()),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto add_point, reloaded->NewUpdateSchema());
  add_point->AddColumn("point", point_struct, "Point struct");
  EXPECT_THAT(add_point->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_point, catalog_->LoadTable(table_ident_));

  // Add point.z - mapping updated automatically on commit
  ICEBERG_UNWRAP_OR_FAIL(auto add_z, with_point->NewUpdateSchema());
  add_z->AddColumn("point", "z", float64(), "Z coordinate");
  EXPECT_THAT(add_z->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_z, catalog_->LoadTable(table_ident_));
  auto mapping_after_z = with_z->metadata()->properties.configs().at(
      std::string(TableProperties::kDefaultNameMapping));
  ICEBERG_UNWRAP_OR_FAIL(auto json2, FromJsonString(mapping_after_z));
  ICEBERG_UNWRAP_OR_FAIL(auto mapping2, NameMappingFromJson(json2));

  auto expected_after_z = MappedFields::Make({
      MappedField{.names = {"x"}, .field_id = 1},
      MappedField{.names = {"y"}, .field_id = 2},
      MappedField{.names = {"z"}, .field_id = 3},
      MappedField{.names = {"point"},
                  .field_id = 4,
                  .nested_mapping = MappedFields::Make({
                      MappedField{.names = {"x"}, .field_id = 5},
                      MappedField{.names = {"y"}, .field_id = 6},
                      MappedField{.names = {"z"}, .field_id = 7},
                  })},
  });
  EXPECT_EQ(mapping2->AsMappedFields(), *expected_after_z);
}

TEST_F(UpdateMappingTest, RenameMappingUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto schema, table_->metadata()->Schema());
  auto initial_mapping = CreateMapping(*schema);
  ASSERT_TRUE(initial_mapping.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto mapping_json, ToJsonString(ToJson(**initial_mapping)));

  ICEBERG_UNWRAP_OR_FAIL(auto props_update, table_->NewUpdateProperties());
  props_update->Set(std::string(TableProperties::kDefaultNameMapping),
                    std::move(mapping_json));
  EXPECT_THAT(props_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  // Rename x -> x_renamed
  ICEBERG_UNWRAP_OR_FAIL(auto rename_update, reloaded->NewUpdateSchema());
  rename_update->RenameColumn("x", "x_renamed");
  EXPECT_THAT(rename_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, catalog_->LoadTable(table_ident_));
  auto updated_mapping_str = updated_table->metadata()->properties.configs().at(
      std::string(TableProperties::kDefaultNameMapping));
  ICEBERG_UNWRAP_OR_FAIL(auto json, FromJsonString(updated_mapping_str));
  ICEBERG_UNWRAP_OR_FAIL(auto updated_mapping, NameMappingFromJson(json));

  // Field 1 should have both names
  EXPECT_THAT(updated_mapping->Find(1)->get().names,
              testing::UnorderedElementsAre("x", "x_renamed"));
}

TEST_F(UpdateMappingTest, RenameNestedFieldMappingUpdate) {
  auto point_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(4, "x", float64()),
      SchemaField::MakeRequired(5, "y", float64()),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto add_point, table_->NewUpdateSchema());
  add_point->AddColumn("point", point_struct, "Point struct");
  EXPECT_THAT(add_point->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_point, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, with_point->metadata()->Schema());
  auto initial_mapping = CreateMapping(*schema);
  ASSERT_TRUE(initial_mapping.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto mapping_json, ToJsonString(ToJson(**initial_mapping)));

  ICEBERG_UNWRAP_OR_FAIL(auto props_update, with_point->NewUpdateProperties());
  props_update->Set(std::string(TableProperties::kDefaultNameMapping),
                    std::move(mapping_json));
  EXPECT_THAT(props_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_mapping, catalog_->LoadTable(table_ident_));

  // Rename point.x -> X, point.y -> Y
  ICEBERG_UNWRAP_OR_FAIL(auto rename_update, with_mapping->NewUpdateSchema());
  rename_update->RenameColumn("point.x", "X").RenameColumn("point.y", "Y");
  EXPECT_THAT(rename_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, catalog_->LoadTable(table_ident_));
  auto updated_mapping_str = updated_table->metadata()->properties.configs().at(
      std::string(TableProperties::kDefaultNameMapping));
  ICEBERG_UNWRAP_OR_FAIL(auto json, FromJsonString(updated_mapping_str));
  ICEBERG_UNWRAP_OR_FAIL(auto updated_mapping, NameMappingFromJson(json));

  auto point_field = updated_mapping->Find("point");
  ASSERT_TRUE(point_field.has_value());
  auto x_field = updated_mapping->Find("point.X");
  ASSERT_TRUE(x_field.has_value());
  auto y_field = updated_mapping->Find("point.Y");
  ASSERT_TRUE(y_field.has_value());
  EXPECT_THAT(x_field->get().names, testing::UnorderedElementsAre("x", "X"));
  EXPECT_THAT(y_field->get().names, testing::UnorderedElementsAre("y", "Y"));
}

TEST_F(UpdateMappingTest, RenameComplexFieldMappingUpdate) {
  auto point_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(4, "x", float64()),
      SchemaField::MakeRequired(5, "y", float64()),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto add_point, table_->NewUpdateSchema());
  add_point->AddColumn("point", point_struct, "Point struct");
  EXPECT_THAT(add_point->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_point, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, with_point->metadata()->Schema());
  auto initial_mapping = CreateMapping(*schema);
  ASSERT_TRUE(initial_mapping.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto mapping_json, ToJsonString(ToJson(**initial_mapping)));

  ICEBERG_UNWRAP_OR_FAIL(auto props_update, with_point->NewUpdateProperties());
  props_update->Set(std::string(TableProperties::kDefaultNameMapping),
                    std::move(mapping_json));
  EXPECT_THAT(props_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto with_mapping, catalog_->LoadTable(table_ident_));

  // Rename point -> p2
  ICEBERG_UNWRAP_OR_FAIL(auto rename_update, with_mapping->NewUpdateSchema());
  rename_update->RenameColumn("point", "p2");
  EXPECT_THAT(rename_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, catalog_->LoadTable(table_ident_));
  auto updated_mapping_str = updated_table->metadata()->properties.configs().at(
      std::string(TableProperties::kDefaultNameMapping));
  ICEBERG_UNWRAP_OR_FAIL(auto json, FromJsonString(updated_mapping_str));
  ICEBERG_UNWRAP_OR_FAIL(auto updated_mapping, NameMappingFromJson(json));

  // Field 4 (point) should have both names
  auto point_field = updated_mapping->Find(4);
  ASSERT_TRUE(point_field.has_value());
  EXPECT_THAT(point_field->get().names, testing::UnorderedElementsAre("point", "p2"));
}

}  // namespace iceberg
