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
#include "roaring/roaring.hh"

#include <gtest/gtest.h>

#include "roaring/roaring64map.hh"

namespace iceberg {

TEST(CRoaringTest, Basic32Bit) {
  roaring::Roaring r1;
  for (uint32_t i = 100; i < 1000; i++) {
    r1.add(i);
  }
  ASSERT_EQ(r1.cardinality(), 900);
  ASSERT_TRUE(r1.contains(500));
  ASSERT_FALSE(r1.contains(50));
  ASSERT_FALSE(r1.isEmpty());
}

TEST(CRoaringTest, Basic64Bit) {
  roaring::Roaring64Map r2;
  for (uint64_t i = 18000000000000000100ull; i < 18000000000000001000ull; i++) {
    r2.add(i);
  }
  ASSERT_EQ(r2.cardinality(), 900);
  ASSERT_TRUE(r2.contains(static_cast<uint64_t>(18000000000000000500ull)));
  ASSERT_FALSE(r2.contains(static_cast<uint64_t>(18000000000000000050ull)));
  ASSERT_FALSE(r2.isEmpty());
}

TEST(CRoaringTest, ConstructorWithInitializerList) {
  roaring::Roaring r1 = roaring::Roaring::bitmapOf(5, 1, 2, 3, 5, 6);
  ASSERT_EQ(r1.cardinality(), 5);
  ASSERT_TRUE(r1.contains(1));
  ASSERT_TRUE(r1.contains(2));
  ASSERT_TRUE(r1.contains(3));
  ASSERT_TRUE(r1.contains(5));
  ASSERT_TRUE(r1.contains(6));
  ASSERT_FALSE(r1.contains(4));
}

}  // namespace iceberg
