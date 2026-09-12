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

#include <cstdint>
#include <iostream>
#include <memory>

// Verify that the installed format dependency interfaces are usable by consumers.
#include <arrow/api.h>
#include <avro/Compiler.hh>
#include <avro/GenericDatum.hh>
#include <parquet/api/reader.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

// The borrowed buffers below remain alive until the writer is closed.
void ReleaseBorrowedArray(ArrowArray* array) {
  for (int64_t i = 0; i < array->n_children; ++i) {
    if (array->children[i]->release) {
      array->children[i]->release(array->children[i]);
    }
  }
  array->release = nullptr;
}

iceberg::Status CheckBundle() {
  auto avro_schema = avro::compileJsonSchemaFromString(R"("int")");
  if (avro::GenericDatum(avro_schema).type() != avro::AVRO_INT) {
    return iceberg::InvalidArgument("Avro schema type did not match");
  }

  iceberg::arrow::RegisterAll();
  iceberg::avro::RegisterAll();
  iceberg::parquet::RegisterAll();
  std::shared_ptr<iceberg::FileIO> io = iceberg::arrow::MakeMockFileIO();
  auto schema = std::make_shared<iceberg::Schema>(std::vector<iceberg::SchemaField>{
      iceberg::SchemaField::MakeRequired(1, "id", iceberg::int32())});
  for (auto format :
       {iceberg::FileFormatType::kAvro, iceberg::FileFormatType::kParquet}) {
    std::string path = "/roundtrip." + std::string(iceberg::ToString(format));
    const int32_t values[] = {42};
    const void* value_buffers[] = {nullptr, values};
    ArrowArray child{.length = 1,
                     .n_buffers = 2,
                     .buffers = value_buffers,
                     .release = ReleaseBorrowedArray};
    ArrowArray* children[] = {&child};
    const void* struct_buffers[] = {nullptr};
    ArrowArray batch{.length = 1,
                     .n_buffers = 1,
                     .n_children = 1,
                     .buffers = struct_buffers,
                     .children = children,
                     .release = ReleaseBorrowedArray};
    auto writer = iceberg::WriterFactoryRegistry::Open(
        format, {.path = path,
                 .schema = schema,
                 .io = io,
                 .properties = iceberg::WriterProperties::FromMap(
                     {{"write.parquet.compression-codec", "uncompressed"}})});
    if (!writer) return std::unexpected(writer.error());
    auto written = (*writer)->Write(&batch);
    if (!written) return written;
    auto closed = (*writer)->Close();
    if (!closed) return closed;
    for (bool skip_datum : {true, false}) {
      iceberg::ReaderProperties properties;
      properties.Set(iceberg::ReaderProperties::kAvroSkipDatum, skip_datum);
      auto reader = iceberg::ReaderFactoryRegistry::Open(
          format,
          {.path = path, .io = io, .projection = schema, .properties = properties});
      if (!reader) return std::unexpected(reader.error());
      auto next = (*reader)->Next();
      if (!next) return std::unexpected(next.error());
      if (!next->has_value()) return iceberg::InvalidArgument("Expected one row");
      auto& output = next->value();
      bool valid = output.length == 1 && output.n_children == 1;
      if (valid) {
        const auto* column = output.children[0];
        valid = column->n_buffers == 2 && column->buffers[1] &&
                static_cast<const int32_t*>(column->buffers[1])[column->offset] == 42;
      }
      output.release(&output);
      if (!valid) return iceberg::InvalidArgument("Round-trip value did not match");
      closed = (*reader)->Close();
      if (!closed) return closed;
      if (format == iceberg::FileFormatType::kParquet) break;
    }
  }
  return {};
}

int main() {
  auto result = CheckBundle();
  if (!result) {
    std::cerr << result.error().message << '\n';
    return 1;
  }
  return 0;
}
