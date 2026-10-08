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

#include <gtest/gtest.h>

#include "iceberg/encryption/key_management_client.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

/// Records the properties it was initialized with; "wraps" by reversing the bytes.
class FakeKms : public KeyManagementClient {
 public:
  Status Initialize(const std::unordered_map<std::string, std::string>& props) override {
    properties = props;
    return {};
  }
  Result<std::vector<uint8_t>> WrapKey(std::span<const uint8_t> key,
                                       std::string_view) override {
    return std::vector<uint8_t>(key.rbegin(), key.rend());
  }
  Result<std::vector<uint8_t>> UnwrapKey(std::span<const uint8_t> wrapped,
                                         std::string_view) override {
    return std::vector<uint8_t>(wrapped.rbegin(), wrapped.rend());
  }

  std::unordered_map<std::string, std::string> properties;
};

void RegisterFake(const std::string& name) {
  KmsRegistry::Register(name, []() -> Result<std::unique_ptr<KeyManagementClient>> {
    return std::make_unique<FakeKms>();
  });
}

}  // namespace

TEST(KmsRegistryTest, NotConfigured) {
  ICEBERG_UNWRAP_OR_FAIL(auto client, KmsRegistry::FromCatalogProperties({}));
  EXPECT_EQ(client, nullptr);
}

TEST(KmsRegistryTest, LoadsCustomImplementation) {
  RegisterFake("fake-kms");
  KmsRegistry::Properties properties{{"encryption.kms-impl", "fake-kms"},
                                     {"fake.endpoint", "localhost"}};
  ICEBERG_UNWRAP_OR_FAIL(auto client, KmsRegistry::FromCatalogProperties(properties));
  ASSERT_NE(client, nullptr);
  auto* fake = dynamic_cast<FakeKms*>(client.get());
  ASSERT_NE(fake, nullptr);
  // Initialized with the catalog properties
  EXPECT_EQ(fake->properties.at("fake.endpoint"), "localhost");

  std::vector<uint8_t> key{1, 2, 3};
  ICEBERG_UNWRAP_OR_FAIL(auto wrapped, client->WrapKey(key, "master"));
  ICEBERG_UNWRAP_OR_FAIL(auto unwrapped, client->UnwrapKey(wrapped, "master"));
  EXPECT_EQ(unwrapped, key);
  EXPECT_FALSE(client->SupportsKeyGeneration());
  EXPECT_THAT(client->GenerateKey("master"), IsError(ErrorKind::kNotSupported));
}

TEST(KmsRegistryTest, LoadsBuiltInType) {
  // Built-in types resolve to the client registered under the type name. The registry
  // is process-wide, so this shadows a real "aws" client for the rest of the binary.
  RegisterFake("aws");
  ICEBERG_UNWRAP_OR_FAIL(
      auto client, KmsRegistry::FromCatalogProperties({{"encryption.kms-type", "AWS"}}));
  EXPECT_NE(dynamic_cast<FakeKms*>(client.get()), nullptr);
}

TEST(KmsRegistryTest, EmptyFactoryIsRejected) {
  // An empty std::function must surface as an error, not a std::bad_function_call
  // escaping through the Result-returning Load.
  KmsRegistry::Register("empty-kms", KmsRegistry::Factory{});
  EXPECT_THAT(KmsRegistry::FromCatalogProperties({{"encryption.kms-impl", "empty-kms"}}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(KmsRegistryTest, InvalidConfiguration) {
  EXPECT_THAT(KmsRegistry::FromCatalogProperties(
                  {{"encryption.kms-type", "aws"}, {"encryption.kms-impl", "fake-kms"}}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(KmsRegistry::FromCatalogProperties({{"encryption.kms-type", "vault"}}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(KmsRegistry::FromCatalogProperties({{"encryption.kms-impl", "missing"}}),
              IsError(ErrorKind::kNotFound));
  // Set but empty is a misconfiguration, not "no KMS", as in Java
  EXPECT_THAT(KmsRegistry::FromCatalogProperties({{"encryption.kms-type", ""}}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(KmsRegistry::FromCatalogProperties({{"encryption.kms-impl", ""}}),
              IsError(ErrorKind::kNotFound));
}

}  // namespace iceberg
