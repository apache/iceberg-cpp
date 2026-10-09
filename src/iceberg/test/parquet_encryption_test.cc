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

#include <memory>
#include <string>

#include <arrow/io/memory.h>
#include <arrow/json/from_string.h>
#include <arrow/table.h>
#include <arrow/util/secure_string.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/encryption/encryption.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

namespace iceberg::parquet {

namespace {

const std::string kKey = "0123456789012345";
const std::string kAadPrefix = "file-aad-prefix-";

std::shared_ptr<::arrow::Table> MakeTable() {
  auto schema = ::arrow::schema({::arrow::field("id", ::arrow::int64())});
  auto ids =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[1, 2, 3]").ValueOrDie();
  return ::arrow::Table::Make(schema, {ids});
}

std::shared_ptr<::arrow::Buffer> WriteEncrypted(::parquet::Compression::type codec) {
  // Same settings as Iceberg: footer key only (encrypted footer), AAD prefix supplied
  // by the reader from the manifest instead of stored in the file.
  ::parquet::FileEncryptionProperties::Builder encryption{
      ::arrow::util::SecureString(std::string(kKey))};
  encryption.aad_prefix(kAadPrefix);
  encryption.disable_aad_prefix_storage();
  auto properties = ::parquet::WriterProperties::Builder()
                        .compression(codec)
                        ->encryption(encryption.build())
                        ->build();

  auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
  PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
      *MakeTable(), ::arrow::default_memory_pool(), sink, 1024, properties));
  return sink->Finish().ValueOrDie();
}

std::shared_ptr<::arrow::Table> Read(const std::shared_ptr<::arrow::Buffer>& buffer,
                                     const std::string& aad_prefix) {
  ::parquet::FileDecryptionProperties::Builder decryption;
  decryption.footer_key(::arrow::util::SecureString(std::string(kKey)));
  decryption.aad_prefix(aad_prefix);
  ::parquet::ReaderProperties properties;
  properties.file_decryption_properties(decryption.build());

  auto reader = ::parquet::ParquetFileReader::Open(
      std::make_shared<::arrow::io::BufferReader>(buffer), properties);
  auto arrow_reader = ::parquet::arrow::FileReader::Make(::arrow::default_memory_pool(),
                                                         std::move(reader))
                          .ValueOrDie();
  return arrow_reader->ReadTable().ValueOrDie();
}

}  // namespace

TEST(ParquetEncryptionTest, ModularEncryptionIsAvailable) {
  for (auto codec :
       {::parquet::Compression::UNCOMPRESSED, ::parquet::Compression::ZSTD}) {
    auto buffer = WriteEncrypted(codec);
    // Encrypted footer mode
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), 4), "PARE");
    EXPECT_TRUE(Read(buffer, kAadPrefix)->Equals(*MakeTable()));
    // The AAD prefix binds the file to its manifest entry
    EXPECT_THROW(Read(buffer, "another-prefix--"), ::parquet::ParquetException);
  }
}

}  // namespace iceberg::parquet
