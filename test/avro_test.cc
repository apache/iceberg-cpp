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
#include <iceberg/avro/demo_avro.h>
#include <iceberg/file_reader.h>

#include "matchers.h"

namespace iceberg::avro {

TEST(AVROTest, TestDemoAvro) {
  std::string expected =
      "{\n\
    \"type\": \"record\",\n\
    \"name\": \"testrecord\",\n\
    \"fields\": [\n\
        {\n\
            \"name\": \"testbytes\",\n\
            \"type\": \"bytes\",\n\
            \"default\": \"\"\n\
        }\n\
    ]\n\
}\n\
";

  auto avro = iceberg::avro::DemoAvro();
  EXPECT_EQ(avro.print(), expected);
}

TEST(AVROTest, TestDemoAvroReader) {
  auto result = ReaderFactoryRegistry::Open(FileFormatType::kAvro, {});
  ASSERT_THAT(result, IsOk());

  auto reader = std::move(result.value());
  ASSERT_EQ(reader->data_layout(), Reader::DataLayout::kStructLike);

  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(std::holds_alternative<std::monostate>(data.value()));
}

}  // namespace iceberg::avro
