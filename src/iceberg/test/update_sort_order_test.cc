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

#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"

namespace iceberg {

class UpdateSortOrderTest : public UpdateTestBase {};

TEST_F(UpdateSortOrderTest, AddSingleSortFieldAscending) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref = NamedReference::Make("x").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order was set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 1);
  EXPECT_EQ(field.direction(), SortDirection::kAscending);
  EXPECT_EQ(field.null_order(), NullOrder::kFirst);
}

TEST_F(UpdateSortOrderTest, AddSingleSortFieldDescending) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref = NamedReference::Make("y").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->AddSortField(std::move(term), SortDirection::kDescending, NullOrder::kLast);
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order was set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 2);
  EXPECT_EQ(field.direction(), SortDirection::kDescending);
  EXPECT_EQ(field.null_order(), NullOrder::kLast);
}

TEST_F(UpdateSortOrderTest, AddMultipleSortFields) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref1 = NamedReference::Make("y").value();
  auto term1 = UnboundTransform::Make(std::move(ref1), Transform::Identity()).value();

  auto ref2 = NamedReference::Make("x").value();
  auto term2 = UnboundTransform::Make(std::move(ref2), Transform::Identity()).value();

  update->AddSortField(std::move(term1), SortDirection::kAscending, NullOrder::kFirst)
      .AddSortField(std::move(term2), SortDirection::kDescending, NullOrder::kLast);

  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order was set with multiple fields
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 2);

  // Check first field (y field)
  const auto& field1 = sort_order->fields()[0];
  EXPECT_EQ(field1.source_id(), 2);
  EXPECT_EQ(field1.direction(), SortDirection::kAscending);
  EXPECT_EQ(field1.null_order(), NullOrder::kFirst);

  // Check second field (x field)
  const auto& field2 = sort_order->fields()[1];
  EXPECT_EQ(field2.source_id(), 1);
  EXPECT_EQ(field2.direction(), SortDirection::kDescending);
  EXPECT_EQ(field2.null_order(), NullOrder::kLast);
}

TEST_F(UpdateSortOrderTest, AddSortFieldWithTruncateTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("x").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Truncate(10)).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order uses truncate transform
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 1);
  EXPECT_EQ(field.transform()->ToString(), "truncate[10]");
  EXPECT_EQ(field.direction(), SortDirection::kAscending);
}

TEST_F(UpdateSortOrderTest, AddSortFieldWithBucketTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("y").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Bucket(10)).value();

  update->AddSortField(std::move(term), SortDirection::kDescending, NullOrder::kLast);
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the sort order uses bucket transform
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 2);
  EXPECT_EQ(field.transform()->ToString(), "bucket[10]");
  EXPECT_EQ(field.direction(), SortDirection::kDescending);
}

TEST_F(UpdateSortOrderTest, AddSortFieldNullTerm) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  update->AddSortField(nullptr, SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Term cannot be null"));
}

TEST_F(UpdateSortOrderTest, AddSortFieldInvalidTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  // Try to apply day transform to a long field (invalid - day requires date/timestamp)
  auto ref = NamedReference::Make("x").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Day()).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("not a valid input type"));
}

TEST_F(UpdateSortOrderTest, AddSortFieldNonExistentField) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("nonexistent").value();
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);

  auto result = update->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot find"));
}

TEST_F(UpdateSortOrderTest, CaseSensitiveTrue) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  auto ref = NamedReference::Make("X").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->CaseSensitive(true).AddSortField(std::move(term), SortDirection::kAscending,
                                           NullOrder::kFirst);

  auto result = update->Commit();
  // Should fail because schema has "x" (lowercase)
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
}

TEST_F(UpdateSortOrderTest, CaseSensitiveFalse) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());
  auto ref = NamedReference::Make("X").value();  // Uppercase
  auto term = UnboundTransform::Make(std::move(ref), Transform::Identity()).value();

  update->CaseSensitive(false).AddSortField(std::move(term), SortDirection::kAscending,
                                            NullOrder::kFirst);

  auto result = update->Commit();
  // Should succeed because case-insensitive matching
  EXPECT_THAT(result, IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);
  EXPECT_EQ(sort_order->fields()[0].source_id(), 1);
}

TEST_F(UpdateSortOrderTest, ReplaceExistingSortOrder) {
  // First, set an initial sort order
  ICEBERG_UNWRAP_OR_FAIL(auto update1, table_->NewUpdateSortOrder());

  auto ref1 = NamedReference::Make("x").value();
  auto term1 = UnboundTransform::Make(std::move(ref1), Transform::Identity()).value();
  update1->AddSortField(std::move(term1), SortDirection::kAscending, NullOrder::kFirst);
  EXPECT_THAT(update1->Commit(), IsOk());

  // Now replace with a different sort order
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  ICEBERG_UNWRAP_OR_FAIL(auto update2, reloaded->NewUpdateSortOrder());
  auto ref2 = NamedReference::Make("y").value();
  auto term2 = UnboundTransform::Make(std::move(ref2), Transform::Identity()).value();
  update2->AddSortField(std::move(term2), SortDirection::kDescending, NullOrder::kLast);
  EXPECT_THAT(update2->Commit(), IsOk());

  // Verify the sort order was replaced
  ICEBERG_UNWRAP_OR_FAIL(auto final_table, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, final_table->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_EQ(sort_order->fields().size(), 1);

  const auto& field = sort_order->fields()[0];
  EXPECT_EQ(field.source_id(), 2);
  EXPECT_EQ(field.direction(), SortDirection::kDescending);
  EXPECT_EQ(field.null_order(), NullOrder::kLast);
}

TEST_F(UpdateSortOrderTest, EmptySortOrder) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSortOrder());

  // Don't add any sort fields
  auto result = update->Commit();

  // Should succeed with an unsorted order
  EXPECT_THAT(result, IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->metadata()->SortOrder());
  ASSERT_NE(sort_order, nullptr);
  EXPECT_TRUE(sort_order->fields().empty());
}

}  // namespace iceberg
