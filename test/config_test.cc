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

#include <gtest/gtest.h>
#include <iceberg/util/config.h>

namespace iceberg {

enum class TestEnum { VALUE1, VALUE2, VALUE3 };

std::string enumToString(const TestEnum& val) {
  switch (val) {
    case TestEnum::VALUE1:
      return "VALUE1";
    case TestEnum::VALUE2:
      return "VALUE2";
    case TestEnum::VALUE3:
      return "VALUE3";
    default:
      throw std::runtime_error("Invalid enum value");
  }
}

TestEnum stringToEnum(const std::string& val) {
  if (val == "VALUE1") {
    return TestEnum::VALUE1;
  } else if (val == "VALUE2") {
    return TestEnum::VALUE2;
  } else if (val == "VALUE3") {
    return TestEnum::VALUE3;
  } else {
    throw std::runtime_error("Invalid enum string");
  }
}

// Define a concrete config class for testing
class TestConfig : public ConfigBase<TestConfig> {
 public:
  template <typename T>
  using Entry = const ConfigBase<TestConfig>::Entry<T>;

  inline static const Entry<std::string> STRING_CONFIG{"string_config", "default_value"};
  inline static const Entry<int> INT_CONFIG{"int_config", 25};
  inline static const Entry<bool> BOOL_CONFIG{"bool_config", false};
  inline static const Entry<TestEnum> ENUM_CONFIG{"enum_config", TestEnum::VALUE1,
                                                  enumToString, stringToEnum};
  inline static const Entry<double> DOUBLE_CONFIG{"double_config", 3.14};
};

TEST(ConfigTest, BasicOperations) {
  TestConfig config;

  // Test default values
  ASSERT_EQ(config.get(TestConfig::STRING_CONFIG), std::string("default_value"));
  ASSERT_EQ(config.get(TestConfig::INT_CONFIG), 25);
  ASSERT_EQ(config.get(TestConfig::BOOL_CONFIG), false);
  ASSERT_EQ(config.get(TestConfig::ENUM_CONFIG), TestEnum::VALUE1);
  ASSERT_EQ(config.get(TestConfig::DOUBLE_CONFIG), 3.14);

  // Test setting values
  config.set(TestConfig::STRING_CONFIG, std::string("new_value"));
  config.set(TestConfig::INT_CONFIG, 100);
  config.set(TestConfig::BOOL_CONFIG, true);
  config.set(TestConfig::ENUM_CONFIG, TestEnum::VALUE2);
  config.set(TestConfig::DOUBLE_CONFIG, 2.71828);

  ASSERT_EQ(config.get(TestConfig::STRING_CONFIG), "new_value");
  ASSERT_EQ(config.get(TestConfig::INT_CONFIG), 100);
  ASSERT_EQ(config.get(TestConfig::BOOL_CONFIG), true);
  ASSERT_EQ(config.get(TestConfig::ENUM_CONFIG), TestEnum::VALUE2);
  ASSERT_EQ(config.get(TestConfig::DOUBLE_CONFIG), 2.71828);

  // Test unsetting a value
  config.unset(TestConfig::INT_CONFIG);
  config.unset(TestConfig::ENUM_CONFIG);
  config.unset(TestConfig::DOUBLE_CONFIG);
  ASSERT_EQ(config.get(TestConfig::INT_CONFIG), 25);
  ASSERT_EQ(config.get(TestConfig::STRING_CONFIG), "new_value");
  ASSERT_EQ(config.get(TestConfig::BOOL_CONFIG), true);
  ASSERT_EQ(config.get(TestConfig::ENUM_CONFIG), TestEnum::VALUE1);
  ASSERT_EQ(config.get(TestConfig::DOUBLE_CONFIG), 3.14);

  // Test resetting all values
  config.reset();
  ASSERT_EQ(config.get(TestConfig::STRING_CONFIG), "default_value");
  ASSERT_EQ(config.get(TestConfig::INT_CONFIG), 25);
  ASSERT_EQ(config.get(TestConfig::BOOL_CONFIG), false);
  ASSERT_EQ(config.get(TestConfig::ENUM_CONFIG), TestEnum::VALUE1);
  ASSERT_EQ(config.get(TestConfig::DOUBLE_CONFIG), 3.14);
}

}  // namespace iceberg
