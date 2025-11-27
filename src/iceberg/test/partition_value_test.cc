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

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

// PartitionValues Tests

TEST(PartitionValuesTest, DefaultConstruction) {
  PartitionValues partition;
  EXPECT_EQ(partition.num_fields(), 0);
}

TEST(PartitionValuesTest, ConstructionFromVector) {
  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));
  EXPECT_EQ(partition.num_fields(), 2);
}

TEST(PartitionValuesTest, ConstructionFromSingleLiteral) {
  PartitionValues partition(Literal::Int(42));
  EXPECT_EQ(partition.num_fields(), 1);
}

TEST(PartitionValuesTest, CopyConstructor) {
  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition1(std::move(values));

  PartitionValues partition2(partition1);
  EXPECT_EQ(partition2.num_fields(), 2);
  EXPECT_EQ(partition1, partition2);
}

TEST(PartitionValuesTest, MoveConstructor) {
  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition1(std::move(values));

  PartitionValues partition2(std::move(partition1));
  EXPECT_EQ(partition2.num_fields(), 2);
}

TEST(PartitionValuesTest, CopyAssignment) {
  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  partition1 = partition2;
  EXPECT_EQ(partition1.num_fields(), 2);
  EXPECT_EQ(partition1, partition2);
}

TEST(PartitionValuesTest, MoveAssignment) {
  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  partition1 = std::move(partition2);
  EXPECT_EQ(partition1.num_fields(), 2);
}

TEST(PartitionValuesTest, GetFieldInt) {
  std::vector<Literal> values = {Literal::Int(42)};
  PartitionValues partition(std::move(values));

  auto result = partition.GetField(0);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::holds_alternative<int32_t>(*result));
  EXPECT_EQ(std::get<int32_t>(*result), 42);
}

TEST(PartitionValuesTest, GetFieldString) {
  std::vector<Literal> values = {Literal::String("hello")};
  PartitionValues partition(std::move(values));

  auto result = partition.GetField(0);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::holds_alternative<std::string_view>(*result));
  EXPECT_EQ(std::get<std::string_view>(*result), "hello");
}

TEST(PartitionValuesTest, GetFieldMultipleTypes) {
  std::vector<Literal> values = {Literal::Int(1),         Literal::Long(2L),
                                 Literal::String("test"), Literal::Boolean(true),
                                 Literal::Float(3.14f),   Literal::Double(2.82e-04)};
  PartitionValues partition(std::move(values));

  auto result0 = partition.GetField(0);
  ASSERT_TRUE(result0.has_value());
  EXPECT_EQ(std::get<int32_t>(*result0), 1);

  auto result1 = partition.GetField(1);
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(std::get<int64_t>(*result1), 2L);

  auto result2 = partition.GetField(2);
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(std::get<std::string_view>(*result2), "test");

  auto result3 = partition.GetField(3);
  ASSERT_TRUE(result3.has_value());
  EXPECT_EQ(std::get<bool>(*result3), true);

  auto result4 = partition.GetField(4);
  ASSERT_TRUE(result4.has_value());
  EXPECT_FLOAT_EQ(std::get<float>(*result4), 3.14f);

  auto result5 = partition.GetField(5);
  ASSERT_TRUE(result5.has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(*result5), 2.82e-04);
}

TEST(PartitionValuesTest, GetFieldOutOfBounds) {
  std::vector<Literal> values = {Literal::Int(1)};
  PartitionValues partition(std::move(values));

  auto result = partition.GetField(1);
  EXPECT_FALSE(result.has_value());
}

TEST(PartitionValuesTest, ValueAt) {
  std::vector<Literal> values = {Literal::Int(42)};
  PartitionValues partition(std::move(values));

  auto result = partition.ValueAt(0);
  ASSERT_TRUE(result.has_value());
  const Literal& literal_ref = result->get();
  EXPECT_EQ(literal_ref, Literal::Int(42));
}

TEST(PartitionValuesTest, ValueOutOfBounds) {
  std::vector<Literal> values = {Literal::Int(1)};
  PartitionValues partition(std::move(values));

  auto result = partition.ValueAt(1);
  EXPECT_FALSE(result.has_value());
}

TEST(PartitionValuesTest, AddValue) {
  PartitionValues partition;
  EXPECT_EQ(partition.num_fields(), 0);

  partition.AddValue(Literal::Int(1));
  EXPECT_EQ(partition.num_fields(), 1);

  partition.AddValue(Literal::String("test"));
  EXPECT_EQ(partition.num_fields(), 2);
}

TEST(PartitionValuesTest, Reset) {
  std::vector<Literal> values1 = {Literal::Int(1)};
  PartitionValues partition(std::move(values1));
  EXPECT_EQ(partition.num_fields(), 1);

  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};
  partition.Reset(std::move(values2));
  EXPECT_EQ(partition.num_fields(), 2);
}

TEST(PartitionValuesTest, EqualityEqual) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  EXPECT_EQ(partition1, partition2);
}

TEST(PartitionValuesTest, EqualityNotEqual) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  EXPECT_NE(partition1, partition2);
}

TEST(PartitionValuesTest, EqualityDifferentSize) {
  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  EXPECT_NE(partition1, partition2);
}

// Hash and Equality Functors Tests

TEST(PartitionValuesHashTest, ConsistentHash) {
  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  PartitionValuesHash hasher;
  size_t hash1 = hasher(partition);
  size_t hash2 = hasher(partition);

  EXPECT_EQ(hash1, hash2);
}

TEST(PartitionValuesHashTest, DifferentPartitionsDifferentHashes) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionValuesHash hasher;
  size_t hash1 = hasher(partition1);
  size_t hash2 = hasher(partition2);

  EXPECT_NE(hash1, hash2);
}

TEST(PartitionValuesHashTest, EqualPartitionsSameHash) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionValuesHash hasher;
  size_t hash1 = hasher(partition1);
  size_t hash2 = hasher(partition2);

  EXPECT_EQ(hash1, hash2);
}

TEST(PartitionValuesEqualTest, EqualPartitions) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionValuesEqual equal;
  EXPECT_TRUE(equal(partition1, partition2));
}

TEST(PartitionValuesEqualTest, NotEqualPartitions) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionValuesEqual equal;
  EXPECT_FALSE(equal(partition1, partition2));
}

TEST(PartitionKeyHashTest, ConsistentHash) {
  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));
  PartitionKey key{1, std::move(partition)};

  PartitionKeyHash hasher;
  size_t hash1 = hasher(key);
  size_t hash2 = hasher(key);

  EXPECT_EQ(hash1, hash2);
}

TEST(PartitionKeyHashTest, DifferentSpecIdsDifferentHashes) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionKey key1{1, std::move(partition1)};
  PartitionKey key2{2, std::move(partition2)};

  PartitionKeyHash hasher;
  size_t hash1 = hasher(key1);
  size_t hash2 = hasher(key2);

  EXPECT_NE(hash1, hash2);
}

TEST(PartitionKeyEqualTest, EqualKeys) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionKey key1{1, std::move(partition1)};
  PartitionKey key2{1, std::move(partition2)};

  PartitionKeyEqual equal;
  EXPECT_TRUE(equal(key1, key2));
}

TEST(PartitionKeyEqualTest, NotEqualKeys) {
  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test")};

  PartitionValues partition1(std::move(values1));
  PartitionValues partition2(std::move(values2));

  PartitionKey key1{1, std::move(partition1)};
  PartitionKey key2{1, std::move(partition2)};

  PartitionKeyEqual equal;
  EXPECT_FALSE(equal(key1, key2));
}

// PartitionMap Tests

TEST(PartitionMapTest, DefaultConstruction) {
  PartitionMap<std::string> map;
  EXPECT_EQ(map.size(), 0);
  EXPECT_TRUE(map.empty());
}

TEST(PartitionMapTest, PutAndGet) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  bool updated = map.put(1, std::move(partition), "value1");
  EXPECT_FALSE(updated);
  EXPECT_EQ(map.size(), 1);

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  auto result = map.get(1, partition2);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().get(), "value1");
}

TEST(PartitionMapTest, PutUpdate) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  map.put(1, std::move(partition), "value1");
  EXPECT_EQ(map.size(), 1);

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  bool updated = map.put(1, std::move(partition2), "value2");
  EXPECT_TRUE(updated);
  EXPECT_EQ(map.size(), 1);

  std::vector<Literal> values3 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition3(std::move(values3));

  auto result = map.get(1, partition3);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().get(), "value2");
}

TEST(PartitionMapTest, GetNonExistent) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  auto result = map.get(1, partition);
  EXPECT_FALSE(result.has_value());
}

TEST(PartitionMapTest, Contains) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  map.put(1, std::move(partition), "value1");

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  EXPECT_TRUE(map.contains(1, partition2));

  std::vector<Literal> values3 = {Literal::Int(2), Literal::String("test")};
  PartitionValues partition3(std::move(values3));

  EXPECT_FALSE(map.contains(1, partition3));
}

TEST(PartitionMapTest, Remove) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  map.put(1, std::move(partition), "value1");
  EXPECT_EQ(map.size(), 1);

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  bool removed = map.remove(1, partition2);
  EXPECT_TRUE(removed);
  EXPECT_EQ(map.size(), 0);
}

TEST(PartitionMapTest, RemoveNonExistent) {
  PartitionMap<std::string> map;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  bool removed = map.remove(1, partition);
  EXPECT_FALSE(removed);
}

TEST(PartitionMapTest, Clear) {
  PartitionMap<std::string> map;

  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test1")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test2")};

  map.put(1, PartitionValues(std::move(values1)), "value1");
  map.put(1, PartitionValues(std::move(values2)), "value2");

  EXPECT_EQ(map.size(), 2);

  map.clear();
  EXPECT_EQ(map.size(), 0);
  EXPECT_TRUE(map.empty());
}

TEST(PartitionMapTest, MultipleSpecIds) {
  PartitionMap<std::string> map;

  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  map.put(1, PartitionValues(std::move(values1)), "spec1_value");
  map.put(2, PartitionValues(std::move(values2)), "spec2_value");

  EXPECT_EQ(map.size(), 2);

  std::vector<Literal> values3 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values4 = {Literal::Int(1), Literal::String("test")};

  auto result1 = map.get(1, PartitionValues(std::move(values3)));
  auto result2 = map.get(2, PartitionValues(std::move(values4)));

  ASSERT_TRUE(result1.has_value());
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result1.value().get(), "spec1_value");
  EXPECT_EQ(result2.value().get(), "spec2_value");
}

TEST(PartitionMapTest, Iteration) {
  PartitionMap<int> map;

  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2)};
  std::vector<Literal> values3 = {Literal::Int(3)};

  map.put(1, PartitionValues(std::move(values1)), 10);
  map.put(1, PartitionValues(std::move(values2)), 20);
  map.put(1, PartitionValues(std::move(values3)), 30);

  int sum = 0;
  for (const auto& [key, value] : map) {
    sum += value;
  }

  EXPECT_EQ(sum, 60);
}

TEST(PartitionMapTest, ConstIteration) {
  PartitionMap<int> map;

  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2)};

  map.put(1, PartitionValues(std::move(values1)), 10);
  map.put(1, PartitionValues(std::move(values2)), 20);

  const auto& const_map = map;
  int sum = 0;
  for (const auto& [key, value] : const_map) {
    sum += value;
  }

  EXPECT_EQ(sum, 30);
}

// PartitionSet Tests

TEST(PartitionSetTest, DefaultConstruction) {
  PartitionSet set;
  EXPECT_EQ(set.size(), 0);
  EXPECT_TRUE(set.empty());
}

TEST(PartitionSetTest, AddElement) {
  PartitionSet set;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  bool added = set.add(1, std::move(partition));
  EXPECT_TRUE(added);
  EXPECT_EQ(set.size(), 1);
}

TEST(PartitionSetTest, AddDuplicate) {
  PartitionSet set;

  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  set.add(1, PartitionValues(std::move(values1)));
  bool added = set.add(1, PartitionValues(std::move(values2)));

  EXPECT_FALSE(added);
  EXPECT_EQ(set.size(), 1);
}

TEST(PartitionSetTest, Contains) {
  PartitionSet set;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  set.add(1, std::move(partition));

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  EXPECT_TRUE(set.contains(1, partition2));

  std::vector<Literal> values3 = {Literal::Int(2), Literal::String("test")};
  PartitionValues partition3(std::move(values3));

  EXPECT_FALSE(set.contains(1, partition3));
}

TEST(PartitionSetTest, Remove) {
  PartitionSet set;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  set.add(1, std::move(partition));
  EXPECT_EQ(set.size(), 1);

  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition2(std::move(values2));

  bool removed = set.remove(1, partition2);
  EXPECT_TRUE(removed);
  EXPECT_EQ(set.size(), 0);
}

TEST(PartitionSetTest, RemoveNonExistent) {
  PartitionSet set;

  std::vector<Literal> values = {Literal::Int(1), Literal::String("test")};
  PartitionValues partition(std::move(values));

  bool removed = set.remove(1, partition);
  EXPECT_FALSE(removed);
}

TEST(PartitionSetTest, Clear) {
  PartitionSet set;

  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test1")};
  std::vector<Literal> values2 = {Literal::Int(2), Literal::String("test2")};

  set.add(1, PartitionValues(std::move(values1)));
  set.add(1, PartitionValues(std::move(values2)));

  EXPECT_EQ(set.size(), 2);

  set.clear();
  EXPECT_EQ(set.size(), 0);
  EXPECT_TRUE(set.empty());
}

TEST(PartitionSetTest, MultipleSpecIds) {
  PartitionSet set;

  std::vector<Literal> values1 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values2 = {Literal::Int(1), Literal::String("test")};

  set.add(1, PartitionValues(std::move(values1)));
  set.add(2, PartitionValues(std::move(values2)));

  EXPECT_EQ(set.size(), 2);

  std::vector<Literal> values3 = {Literal::Int(1), Literal::String("test")};
  std::vector<Literal> values4 = {Literal::Int(1), Literal::String("test")};

  EXPECT_TRUE(set.contains(1, PartitionValues(std::move(values3))));
  EXPECT_TRUE(set.contains(2, PartitionValues(std::move(values4))));
}

TEST(PartitionSetTest, Iteration) {
  PartitionSet set;

  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2)};
  std::vector<Literal> values3 = {Literal::Int(3)};

  set.add(1, PartitionValues(std::move(values1)));
  set.add(1, PartitionValues(std::move(values2)));
  set.add(1, PartitionValues(std::move(values3)));

  int count = 0;
  for (const auto& [spec_id, partition] : set) {
    EXPECT_EQ(spec_id, 1);
    count++;
  }

  EXPECT_EQ(count, 3);
}

TEST(PartitionSetTest, ConstIteration) {
  PartitionSet set;

  std::vector<Literal> values1 = {Literal::Int(1)};
  std::vector<Literal> values2 = {Literal::Int(2)};

  set.add(1, PartitionValues(std::move(values1)));
  set.add(1, PartitionValues(std::move(values2)));

  const auto& const_set = set;
  int count = 0;
  for (const auto& [spec_id, partition] : const_set) {
    EXPECT_EQ(spec_id, 1);
    count++;
  }

  EXPECT_EQ(count, 2);
}

}  // namespace iceberg
