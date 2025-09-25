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

#include "iceberg/util/config.h"

#include <string>

#include <gtest/gtest.h>

namespace iceberg {

enum class TestEnum { VALUE1, VALUE2, VALUE3 };

std::string EnumToString(const TestEnum& val) {
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

TestEnum StringToEnum(const std::string& val) {
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

  inline static const Entry<std::string> kStringConfig{"string_config", "default_value"};
  inline static const Entry<int> kIntConfig{"int_config", 25};
  inline static const Entry<bool> kBoolConfig{"bool_config", false};
  inline static const Entry<TestEnum> kEnumConfig{"enum_config", TestEnum::VALUE1,
                                                  EnumToString, StringToEnum};
  inline static const Entry<double> kDoubleConfig{"double_config", 3.14};
};

TEST(ConfigTest, BasicOperations) {
  TestConfig config;

  // Test default values
  ASSERT_EQ(config.Get(TestConfig::kStringConfig), std::string("default_value"));
  ASSERT_EQ(config.Get(TestConfig::kIntConfig), 25);
  ASSERT_EQ(config.Get(TestConfig::kBoolConfig), false);
  ASSERT_EQ(config.Get(TestConfig::kEnumConfig), TestEnum::VALUE1);
  ASSERT_EQ(config.Get(TestConfig::kDoubleConfig), 3.14);

  // Test setting values
  config.Set(TestConfig::kStringConfig, std::string("new_value"));
  config.Set(TestConfig::kIntConfig, 100);
  config.Set(TestConfig::kBoolConfig, true);
  config.Set(TestConfig::kEnumConfig, TestEnum::VALUE2);
  config.Set(TestConfig::kDoubleConfig, 2.99);

  ASSERT_EQ(config.Get(TestConfig::kStringConfig), "new_value");
  ASSERT_EQ(config.Get(TestConfig::kIntConfig), 100);
  ASSERT_EQ(config.Get(TestConfig::kBoolConfig), true);
  ASSERT_EQ(config.Get(TestConfig::kEnumConfig), TestEnum::VALUE2);
  ASSERT_EQ(config.Get(TestConfig::kDoubleConfig), 2.99);

  // Test unsetting a value
  config.Unset(TestConfig::kIntConfig);
  config.Unset(TestConfig::kEnumConfig);
  config.Unset(TestConfig::kDoubleConfig);
  ASSERT_EQ(config.Get(TestConfig::kIntConfig), 25);
  ASSERT_EQ(config.Get(TestConfig::kStringConfig), "new_value");
  ASSERT_EQ(config.Get(TestConfig::kBoolConfig), true);
  ASSERT_EQ(config.Get(TestConfig::kEnumConfig), TestEnum::VALUE1);
  ASSERT_EQ(config.Get(TestConfig::kDoubleConfig), 3.14);

  // Test resetting all values
  config.Reset();
  ASSERT_EQ(config.Get(TestConfig::kStringConfig), "default_value");
  ASSERT_EQ(config.Get(TestConfig::kIntConfig), 25);
  ASSERT_EQ(config.Get(TestConfig::kBoolConfig), false);
  ASSERT_EQ(config.Get(TestConfig::kEnumConfig), TestEnum::VALUE1);
  ASSERT_EQ(config.Get(TestConfig::kDoubleConfig), 3.14);
}

}  // namespace iceberg
