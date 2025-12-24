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

#include "iceberg/update/update_sort_order.h"

#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"

namespace iceberg {

class UpdateSortOrderTest : public UpdateTestBase {
 protected:
  // Helper function to apply update and verify the resulting sort order
  void ApplyAndExpectSortOrder(UpdateSortOrder* update,
                               std::vector<SortField> expected_fields) {
    ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_sort_order,
        SortOrder::Make(result->order_id(), std::move(expected_fields)));
    EXPECT_EQ(*result, *expected_sort_order);
  }
};

TEST_F(UpdateSortOrderTest, EmptySortOrder) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  // Should succeed with an unsorted order
  EXPECT_TRUE(result->fields().empty());
}

TEST_F(UpdateSortOrderTest, AddSingleSortFieldAscending) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto term = Expressions::Transform("x", Transform::Identity());
  update->AddSortField(term, SortDirection::kAscending, NullOrder::kFirst);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSingleSortFieldDescending) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto term = Expressions::Transform("y", Transform::Identity());
  update->AddSortField(term, SortDirection::kDescending, NullOrder::kLast);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(2, Transform::Identity(), SortDirection::kDescending,
                               NullOrder::kLast);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddMultipleSortFields) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto term1 = Expressions::Transform("y", Transform::Identity());
  auto term2 = Expressions::Transform("x", Transform::Identity());
  update->AddSortField(term1, SortDirection::kAscending, NullOrder::kFirst)
      .AddSortField(term2, SortDirection::kDescending, NullOrder::kLast);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(2, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kDescending,
                               NullOrder::kLast);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSortFieldWithNamedReference) {
  // Test that we can directly use NamedReference (kReference) which is treated as
  // identity
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref = Expressions::Ref("x");
  update->AddSortField(ref, SortDirection::kAscending, NullOrder::kFirst);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSortFieldByName) {
  // Test the convenience method for adding sort field by name
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  update->AddSortFieldByName("x", SortDirection::kAscending, NullOrder::kFirst);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSortFieldWithTruncateTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto term = Expressions::Truncate("x", 10);
  update->AddSortField(term, SortDirection::kAscending, NullOrder::kFirst);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Truncate(10), SortDirection::kAscending,
                               NullOrder::kFirst);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSortFieldWithBucketTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto term = Expressions::Bucket("y", 10);
  update->AddSortField(term, SortDirection::kDescending, NullOrder::kLast);

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(2, Transform::Bucket(10), SortDirection::kDescending,
                               NullOrder::kLast);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, AddSortFieldNullTerm) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  update->AddSortField(nullptr, SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Term cannot be null"));
}

TEST_F(UpdateSortOrderTest, AddSortFieldInvalidTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  // Try to apply day transform to a long field (invalid - day requires date/timestamp)
  auto ref = NamedReference::Make("x").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Day()).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("not a valid input type"));
}

TEST_F(UpdateSortOrderTest, AddSortFieldNonExistentField) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("nonexistent").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot find"));
}

TEST_F(UpdateSortOrderTest, CaseSensitiveTrue) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("X").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->CaseSensitive(true).AddSortField(std::move(term), SortDirection::kAscending,
                                           NullOrder::kFirst);

  auto result = update->Apply();
  // Should fail because schema has "x" (lowercase)
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
}

TEST_F(UpdateSortOrderTest, CaseSensitiveFalse) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref = NamedReference::Make("X").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();
  update->CaseSensitive(false).AddSortField(std::move(term), SortDirection::kAscending,
                                            NullOrder::kFirst);

  // Should succeed because case-insensitive matching
  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ApplyAndExpectSortOrder(update.get(), std::move(expected_fields));
}

TEST_F(UpdateSortOrderTest, CommitSuccess) {
  // Test empty commit
  ICEBERG_UNWRAP_OR_FAIL(auto empty_update, table_->NewUpdateSortOrder());
  EXPECT_THAT(empty_update->Commit(), IsOk());

  // Reload table after first commit
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  // Test commit with sort order changes
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateSortOrder());
  auto ref = NamedReference::Make("x").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();
  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);

  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order was committed
  ICEBERG_UNWRAP_OR_FAIL(auto final_table, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, final_table->sort_order());

  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_sort_order,
      SortOrder::Make(sort_order->order_id(), std::move(expected_fields)));
  EXPECT_EQ(*sort_order, *expected_sort_order);
}

}  // namespace iceberg
