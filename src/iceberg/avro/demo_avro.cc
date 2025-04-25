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

#include "iceberg/avro/demo_avro.h"

#include <sstream>

#include "avro/Compiler.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/demo.h"

namespace iceberg::avro {

std::string DemoAvro::print() const {
  std::string input =
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

  ::avro::ValidSchema schema = ::avro::compileJsonSchemaFromString(input);
  std::ostringstream actual;
  schema.toJson(actual);

  return actual.str();
}

Result<Reader::Data> DemoAvroReader::Next() { return std::monostate(); }

Reader::DataLayout DemoAvroReader::data_layout() const {
  return Reader::DataLayout::kStructLike;
}

ICEBERG_REGISTER_READER_FACTORY(
    Avro, [](const ReaderOptions& options) -> Result<std::unique_ptr<Reader>> {
      return std::make_unique<DemoAvroReader>();
    });

}  // namespace iceberg::avro
