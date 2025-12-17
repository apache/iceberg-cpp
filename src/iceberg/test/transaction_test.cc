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

#include "iceberg/transaction.h"

#include "iceberg/table.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {

class TransactionTest : public UpdateTestBase {};

TEST_F(TransactionTest, CreateTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  EXPECT_NE(txn, nullptr);
  EXPECT_EQ(txn->table(), table_);
}

TEST_F(TransactionTest, UpdatePropertiesInTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());

  update->Set("key1", "value1");
  EXPECT_THAT(update->Apply(), IsOk());
}

TEST_F(TransactionTest, CommitEmptyTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  EXPECT_THAT(txn->Commit(), IsOk());
}

TEST_F(TransactionTest, CommitTransactionWithPropertyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());

  update->Set("txn.property", "txn.value");
  EXPECT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, txn->Commit());
  EXPECT_NE(updated_table, nullptr);

  // Reload table and verify the property was set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("txn.property"), "txn.value");
}

TEST_F(TransactionTest, MultipleUpdatesInTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());

  // First update
  ICEBERG_UNWRAP_OR_FAIL(auto update1, txn->NewUpdateProperties());
  update1->Set("key1", "value1");
  EXPECT_THAT(update1->Commit(), IsOk());

  // Second update
  ICEBERG_UNWRAP_OR_FAIL(auto update2, txn->NewUpdateProperties());
  update2->Set("key2", "value2");
  EXPECT_THAT(update2->Commit(), IsOk());

  // Commit transaction
  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, txn->Commit());

  // Verify both properties were set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("key1"), "value1");
  EXPECT_EQ(props.at("key2"), "value2");
}

}  // namespace iceberg
