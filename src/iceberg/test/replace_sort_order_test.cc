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

#include "iceberg/update/replace_sort_order.h"

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/transform.h"

namespace iceberg {

class ReplaceSortOrderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple schema with various field types
    SchemaField field1(1, "id", std::make_shared<LongType>(), false);
    SchemaField field2(2, "name", std::make_shared<StringType>(), true);
    SchemaField field3(3, "ts", std::make_shared<TimestampType>(), true);
    SchemaField field4(4, "price", std::make_shared<DecimalType>(10, 2), true);
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{field1, field2, field3, field4}, 1);

    // Create basic table metadata
    metadata_ = std::make_shared<TableMetadata>();
    metadata_->schemas.push_back(schema_);
    metadata_->current_schema_id = 1;

    // Create catalog and table identifier
    catalog_ = std::make_shared<MockCatalog>();
    identifier_ = TableIdentifier(Namespace({"test"}), "table");
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<MockCatalog> catalog_;
  TableIdentifier identifier_;
};

TEST_F(ReplaceSortOrderTest, AscendingSingleField) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("id").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update.Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 1);
  EXPECT_EQ(field.direction(), SortDirection::kAscending);
  EXPECT_EQ(field.null_order(), NullOrder::kFirst);
}

TEST_F(ReplaceSortOrderTest, DescendingSingleField) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("name").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update.Desc(std::move(term), NullOrder::kLast);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 2);
  EXPECT_EQ(field.direction(), SortDirection::kDescending);
  EXPECT_EQ(field.null_order(), NullOrder::kLast);
}

TEST_F(ReplaceSortOrderTest, MultipleFields) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref1 = NamedReference::Make("name").value();
  auto term1 = UnboundTransform::Make(std::move(ref1), Transform::Identity()).value();

  auto ref2 = NamedReference::Make("id").value();
  auto term2 = UnboundTransform::Make(std::move(ref2), Transform::Identity()).value();

  update.Asc(std::move(term1), NullOrder::kFirst)
      .Desc(std::move(term2), NullOrder::kLast);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 2);

  // Check first field
  const auto& field1 = sort_order->fields()[0];
  EXPECT_EQ(field1.source_id(), 2);  // name field
  EXPECT_EQ(field1.direction(), SortDirection::kAscending);
  EXPECT_EQ(field1.null_order(), NullOrder::kFirst);

  // Check second field
  const auto& field2 = sort_order->fields()[1];
  EXPECT_EQ(field2.source_id(), 1);  // id field
  EXPECT_EQ(field2.direction(), SortDirection::kDescending);
  EXPECT_EQ(field2.null_order(), NullOrder::kLast);
}

TEST_F(ReplaceSortOrderTest, WithTransform) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("ts").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Day()).value();

  update.Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 3);
  EXPECT_EQ(field.transform()->ToString(), "day");
  EXPECT_EQ(field.direction(), SortDirection::kAscending);
  EXPECT_EQ(field.null_order(), NullOrder::kFirst);
}

TEST_F(ReplaceSortOrderTest, WithBucketTransform) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("name").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Bucket(10)).value();

  update.Desc(std::move(term), NullOrder::kLast);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 2);
  EXPECT_EQ(field.transform()->ToString(), "bucket[10]");
  EXPECT_EQ(field.direction(), SortDirection::kDescending);
  EXPECT_EQ(field.null_order(), NullOrder::kLast);
}

TEST_F(ReplaceSortOrderTest, NullTerm) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  update.Asc(nullptr, NullOrder::kFirst);

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Term cannot be null"));
}

TEST_F(ReplaceSortOrderTest, InvalidTransformForType) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  // Try to apply day transform to a string field (invalid)
  auto ref = NamedReference::Make("name").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Day()).value();

  update.Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("not a valid input type"));
}

TEST_F(ReplaceSortOrderTest, NonExistentField) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("nonexistent").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update.Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot find"));
}

TEST_F(ReplaceSortOrderTest, CaseSensitiveTrue) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("ID").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update.CaseSensitive(true).Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  // Should fail because schema has "id" (lowercase)
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
}

TEST_F(ReplaceSortOrderTest, CaseSensitiveFalse) {
  ReplaceSortOrder update(identifier_, catalog_, metadata_);

  auto ref = NamedReference::Make("ID").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update.CaseSensitive(false).Asc(std::move(term), NullOrder::kFirst);

  auto result = update.Apply();
  // Should succeed because case-insensitive matching
  EXPECT_THAT(result, IsOk());

  auto sort_order = update.GetBuiltSortOrder();
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);
  EXPECT_EQ(sort_order->fields()[0].source_id(), 1);
}

}  // namespace iceberg
