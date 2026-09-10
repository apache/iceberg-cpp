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

#include "iceberg/catalog/catalog_util.h"

#include <gtest/gtest.h>

#include "iceberg/table_identifier.h"

namespace iceberg {

class CatalogUtilTest : public ::testing::Test {
 protected:
  TableIdentifier identifier_ = {
      .ns = Namespace{.levels = {"db"}},
      .name = "test_table",
  };
};

TEST_F(CatalogUtilTest, EmptyCatalogName) {
  EXPECT_EQ(CatalogUtil::FullTableName("", identifier_), "db.test_table");
}

TEST_F(CatalogUtilTest, DotJoinedCatalogName) {
  EXPECT_EQ(CatalogUtil::FullTableName("my_catalog", identifier_),
            "my_catalog.db.test_table");
}

TEST_F(CatalogUtilTest, UriCatalogNameWithoutTrailingSlash) {
  EXPECT_EQ(CatalogUtil::FullTableName("thrift://localhost:9083", identifier_),
            "thrift://localhost:9083/db.test_table");
}

TEST_F(CatalogUtilTest, UriCatalogNameWithTrailingSlash) {
  EXPECT_EQ(CatalogUtil::FullTableName("hdfs://nameservice/warehouse/", identifier_),
            "hdfs://nameservice/warehouse/db.test_table");
}

TEST_F(CatalogUtilTest, PathStyleCatalogName) {
  EXPECT_EQ(CatalogUtil::FullTableName("/test/catalog", identifier_),
            "/test/catalog/db.test_table");
}

TEST_F(CatalogUtilTest, MultiLevelNamespace) {
  TableIdentifier identifier{
      .ns = Namespace{.levels = {"analytics", "prod"}},
      .name = "events",
  };
  EXPECT_EQ(CatalogUtil::FullTableName("catalog", identifier),
            "catalog.analytics.prod.events");
}

}  // namespace iceberg
