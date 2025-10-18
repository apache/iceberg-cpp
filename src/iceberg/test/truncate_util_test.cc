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

#include "iceberg/util/truncate_util.h"

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"

namespace iceberg {

// The following tests are from
// https://iceberg.apache.org/spec/#truncate-transform-details
TEST(TruncateUtilTest, TruncateLiteral) {
  // Integer
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Int(1), 10), Literal::Int(0));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Int(-1), 10), Literal::Int(-10));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Long(1), 10), Literal::Long(0));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Long(-1), 10), Literal::Long(-10));

  // Decimal
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Decimal(1065, 4, 2), 50),
            Literal::Decimal(1050, 4, 2));

  // String
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::String("iceberg"), 3),
            Literal::String("ice"));

  // Binary
  std::string data = "\x01\x02\x03\x04\x05";
  std::string expected = "\x01\x02\x03";
  EXPECT_EQ(TruncateUtils::TruncateLiteral(
                Literal::Binary(std::vector<uint8_t>(data.begin(), data.end())), 3),
            Literal::Binary(std::vector<uint8_t>(expected.begin(), expected.end())));
}

}  // namespace iceberg
