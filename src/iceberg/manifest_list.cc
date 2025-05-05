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

#include "iceberg/manifest_list.h"

#include "iceberg/schema.h"
#include "iceberg/type.h"

namespace iceberg {

const SchemaField FieldSummary::CONTAINS_NULL =
    SchemaField::MakeRequired(509, "contains_null", std::make_shared<BooleanType>());
const SchemaField FieldSummary::CONTAINS_NAN =
    SchemaField::MakeOptional(518, "contains_nan", std::make_shared<BooleanType>());
const SchemaField FieldSummary::LOWER_BOUND =
    SchemaField::MakeOptional(510, "lower_bound", std::make_shared<BinaryType>());
const SchemaField FieldSummary::UPPER_BOUND =
    SchemaField::MakeOptional(511, "upper_bound", std::make_shared<BinaryType>());

StructType FieldSummary::GetType() {
  return StructType({
      CONTAINS_NULL,
      CONTAINS_NAN,
      LOWER_BOUND,
      UPPER_BOUND,
  });
}

const SchemaField ManifestFile::MANIFEST_PATH =
    SchemaField::MakeRequired(500, "manifest_path", std::make_shared<StringType>());
const SchemaField ManifestFile::MANIFEST_LENGTH =
    SchemaField::MakeRequired(501, "manifest_length", std::make_shared<LongType>());
const SchemaField ManifestFile::PARTITION_SPEC_ID =
    SchemaField::MakeRequired(502, "partition_spec_id", std::make_shared<IntType>());
const SchemaField ManifestFile::CONTENT =
    SchemaField::MakeOptional(517, "content", std::make_shared<IntType>());
const SchemaField ManifestFile::SEQUENCE_NUMBER =
    SchemaField::MakeOptional(515, "sequence_number", std::make_shared<LongType>());
const SchemaField ManifestFile::MIN_SEQUENCE_NUMBER =
    SchemaField::MakeOptional(516, "min_sequence_number", std::make_shared<LongType>());
const SchemaField ManifestFile::ADDED_SNAPSHOT_ID =
    SchemaField::MakeRequired(503, "added_snapshot_id", std::make_shared<LongType>());
const SchemaField ManifestFile::ADDED_FILES_COUNT =
    SchemaField::MakeOptional(504, "added_files_count", std::make_shared<IntType>());
const SchemaField ManifestFile::EXISTING_FILES_COUNT =
    SchemaField::MakeOptional(505, "existing_files_count", std::make_shared<IntType>());
const SchemaField ManifestFile::DELETED_FILES_COUNT =
    SchemaField::MakeOptional(506, "deleted_files_count", std::make_shared<IntType>());
const SchemaField ManifestFile::ADDED_ROWS_COUNT =
    SchemaField::MakeOptional(512, "added_rows_count", std::make_shared<LongType>());
const SchemaField ManifestFile::EXISTING_ROWS_COUNT =
    SchemaField::MakeOptional(513, "existing_rows_count", std::make_shared<LongType>());
const SchemaField ManifestFile::DELETED_ROWS_COUNT =
    SchemaField::MakeOptional(514, "deleted_rows_count", std::make_shared<LongType>());
const SchemaField ManifestFile::PARTITIONS = SchemaField::MakeOptional(
    507, "partitions",
    std::make_shared<ListType>(SchemaField::MakeRequired(
        508, std::string(ListType::kElementName),
        std::make_shared<StructType>(FieldSummary::GetType()))));
const SchemaField ManifestFile::KEY_METADATA =
    SchemaField::MakeOptional(519, "key_metadata", std::make_shared<BinaryType>());
const SchemaField ManifestFile::FIRST_ROW_ID =
    SchemaField::MakeOptional(520, "first_row_id", std::make_shared<LongType>());

Schema ManifestFile::schema() {
  std::vector<SchemaField> fields;
  fields.push_back(MANIFEST_PATH);
  fields.push_back(MANIFEST_LENGTH);
  fields.push_back(PARTITION_SPEC_ID);
  fields.push_back(CONTENT);
  fields.push_back(SEQUENCE_NUMBER);
  fields.push_back(MIN_SEQUENCE_NUMBER);
  fields.push_back(ADDED_SNAPSHOT_ID);
  fields.push_back(ADDED_FILES_COUNT);
  fields.push_back(EXISTING_FILES_COUNT);
  fields.push_back(DELETED_FILES_COUNT);
  fields.push_back(ADDED_ROWS_COUNT);
  fields.push_back(EXISTING_ROWS_COUNT);
  fields.push_back(DELETED_ROWS_COUNT);
  fields.push_back(PARTITIONS);
  fields.push_back(KEY_METADATA);
  fields.push_back(FIRST_ROW_ID);

  return {std::move(fields), /*schema_id=*/std::nullopt};
}

}  // namespace iceberg
