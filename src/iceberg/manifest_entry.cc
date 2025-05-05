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

#include "iceberg/manifest_entry.h"

#include <memory>
#include <optional>
#include <vector>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {
const SchemaField DataFile::CONTENT =
    SchemaField::MakeRequired(134, "content", std::make_shared<IntType>());
const SchemaField DataFile::FILE_PATH =
    SchemaField::MakeRequired(100, "file_path", std::make_shared<StringType>());
const SchemaField DataFile::FILE_FORMAT =
    SchemaField::MakeRequired(101, "file_format", std::make_shared<IntType>());
const SchemaField DataFile::RECORD_COUNT =
    SchemaField::MakeRequired(103, "record_count", std::make_shared<LongType>());
const SchemaField DataFile::FILE_SIZE =
    SchemaField::MakeRequired(104, "file_size_in_bytes", std::make_shared<LongType>());
const SchemaField DataFile::COLUMN_SIZES = SchemaField::MakeOptional(
    108, "column_sizes",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(117, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(118, std::string(MapType::kValueName),
                                  std::make_shared<LongType>())));
const SchemaField DataFile::VALUE_COUNTS = SchemaField::MakeOptional(
    109, "value_counts",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(119, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(120, std::string(MapType::kValueName),
                                  std::make_shared<LongType>())));
const SchemaField DataFile::NULL_VALUE_COUNTS = SchemaField::MakeOptional(
    110, "null_value_counts",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(121, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(122, std::string(MapType::kValueName),
                                  std::make_shared<LongType>())));
const SchemaField DataFile::NAN_VALUE_COUNTS = SchemaField::MakeOptional(
    137, "nan_value_counts",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(138, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(139, std::string(MapType::kValueName),
                                  std::make_shared<LongType>())));
const SchemaField DataFile::LOWER_BOUNDS = SchemaField::MakeOptional(
    125, "lower_bounds",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(126, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(127, std::string(MapType::kValueName),
                                  std::make_shared<BinaryType>())));
const SchemaField DataFile::UPPER_BOUNDS = SchemaField::MakeOptional(
    128, "upper_bounds",
    std::make_shared<MapType>(
        SchemaField::MakeRequired(129, std::string(MapType::kKeyName),
                                  std::make_shared<IntType>()),
        SchemaField::MakeRequired(130, std::string(MapType::kValueName),
                                  std::make_shared<BinaryType>())));
const SchemaField DataFile::KEY_METADATA =
    SchemaField::MakeOptional(131, "key_metadata", std::make_shared<BinaryType>());
const SchemaField DataFile::SPLIT_OFFSETS = SchemaField::MakeOptional(
    132, "split_offsets",
    std::make_shared<ListType>(SchemaField::MakeRequired(
        133, std::string(ListType::kElementName), std::make_shared<LongType>())));
const SchemaField DataFile::EQUALITY_IDS = SchemaField::MakeOptional(
    135, "equality_ids",
    std::make_shared<ListType>(SchemaField::MakeRequired(
        136, std::string(ListType::kElementName), std::make_shared<IntType>())));
const SchemaField DataFile::SORT_ORDER_ID =
    SchemaField::MakeOptional(140, "sort_order_id", std::make_shared<IntType>());
const SchemaField DataFile::FIRST_ROW_ID =
    SchemaField::MakeOptional(142, "first_row_id", std::make_shared<LongType>());
const SchemaField DataFile::REFERENCED_DATA_FILE = SchemaField::MakeOptional(
    143, "referenced_data_file", std::make_shared<StringType>());
const SchemaField DataFile::CONTENT_OFFSET =
    SchemaField::MakeOptional(144, "content_offset", std::make_shared<LongType>());
const SchemaField DataFile::CONTENT_SIZE =
    SchemaField::MakeOptional(145, "content_size_in_bytes", std::make_shared<LongType>());

StructType DataFile::GetType(StructType partition_type) {
  std::vector<SchemaField> fields;

  fields.push_back(CONTENT);
  fields.push_back(FILE_PATH);
  fields.push_back(FILE_FORMAT);
  fields.push_back(SchemaField::MakeRequired(
      102, "partition", std::make_shared<StructType>(partition_type)));
  fields.push_back(RECORD_COUNT);
  fields.push_back(FILE_SIZE);
  fields.push_back(COLUMN_SIZES);
  fields.push_back(VALUE_COUNTS);
  fields.push_back(NULL_VALUE_COUNTS);
  fields.push_back(NAN_VALUE_COUNTS);
  fields.push_back(LOWER_BOUNDS);
  fields.push_back(UPPER_BOUNDS);
  fields.push_back(KEY_METADATA);
  fields.push_back(SPLIT_OFFSETS);
  fields.push_back(EQUALITY_IDS);
  fields.push_back(SORT_ORDER_ID);
  fields.push_back(FIRST_ROW_ID);
  fields.push_back(REFERENCED_DATA_FILE);
  fields.push_back(CONTENT_OFFSET);
  fields.push_back(CONTENT_SIZE);

  return StructType(std::move(fields));
}

const SchemaField ManifestEntry::STATUS =
    SchemaField::MakeRequired(0, "status", std::make_shared<IntType>());
const SchemaField ManifestEntry::SNAPSHOT_ID =
    SchemaField::MakeOptional(1, "snapshot_id", std::make_shared<LongType>());
const SchemaField ManifestEntry::SEQUENCE_NUMBER =
    SchemaField::MakeOptional(3, "sequence_number", std::make_shared<LongType>());
const SchemaField ManifestEntry::FILE_SEQUENCE_NUMBER =
    SchemaField::MakeOptional(4, "file_sequence_number", std::make_shared<LongType>());

Schema ManifestEntry::GetSchema(StructType partition_type) {
  return GetSchemaFromDataFileType(DataFile::GetType(partition_type));
}

Schema ManifestEntry::GetSchemaFromDataFileType(StructType datafile_type) {
  std::vector<SchemaField> fields;

  fields.push_back(STATUS);
  fields.push_back(SNAPSHOT_ID);
  fields.push_back(SEQUENCE_NUMBER);
  fields.push_back(FILE_SEQUENCE_NUMBER);

  // Add the data file schema
  auto data_file_type_field = SchemaField::MakeRequired(
      2, "data_file", std::make_shared<StructType>(DataFile::GetType(datafile_type)));
  fields.push_back(data_file_type_field);

  return {std::move(fields), /*schema_id=*/std::nullopt};
}

}  // namespace iceberg
