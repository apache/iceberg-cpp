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

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

class SnapshotSummaryBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple schema
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()),
        SchemaField::MakeRequired(2, "data", string()),
    });

    // Create unpartitioned spec
    spec_ = PartitionSpec::Unpartitioned();
  }

  DataFile CreateDataFile(const std::string& path, int64_t size, int64_t record_count) {
    DataFile file;
    file.content = DataFile::Content::kData;
    file.file_path = path;
    file.file_format = FileFormatType::kParquet;
    file.file_size_in_bytes = size;
    file.record_count = record_count;
    file.partition = PartitionValues{};
    return file;
  }

  DataFile CreatePosDeleteFile(const std::string& path, int64_t size,
                               int64_t record_count) {
    DataFile file;
    file.content = DataFile::Content::kPositionDeletes;
    file.file_path = path;
    file.file_format = FileFormatType::kParquet;
    file.file_size_in_bytes = size;
    file.record_count = record_count;
    file.partition = PartitionValues{};
    return file;
  }

  DataFile CreateEqDeleteFile(const std::string& path, int64_t size,
                              int64_t record_count) {
    DataFile file;
    file.content = DataFile::Content::kEqualityDeletes;
    file.file_path = path;
    file.file_format = FileFormatType::kParquet;
    file.file_size_in_bytes = size;
    file.record_count = record_count;
    file.partition = PartitionValues{};
    file.equality_ids = {1};  // Delete on column 1
    return file;
  }

  DataFile CreateDeletionVector(const std::string& path, int64_t size,
                                int64_t record_count) {
    DataFile file;
    file.content = DataFile::Content::kPositionDeletes;
    file.file_path = path;
    file.file_format = FileFormatType::kPuffin;  // DVs use Puffin format
    file.file_size_in_bytes = size;
    file.record_count = record_count;
    file.partition = PartitionValues{};
    return file;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
};

// Test basic file addition
TEST_F(SnapshotSummaryBuilderTest, AddDataFile) {
  SnapshotSummaryBuilder builder;

  auto file = CreateDataFile("/path/to/data.parquet", 1000, 100);
  ASSERT_THAT(builder.AddedFile(*spec_, file), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "100");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "1000");
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kDeletedDataFiles), 0);
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kRemovedFileSize), 0);
}

// Test basic file deletion
TEST_F(SnapshotSummaryBuilderTest, DeleteDataFile) {
  SnapshotSummaryBuilder builder;

  auto file = CreateDataFile("/path/to/data.parquet", 1000, 100);
  ASSERT_THAT(builder.DeletedFile(*spec_, file), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedDataFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedRecords], "100");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedFileSize], "1000");
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kAddedDataFiles), 0);
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kAddedFileSize), 0);
}

// Test adding and removing files together
TEST_F(SnapshotSummaryBuilderTest, AddAndDeleteFiles) {
  SnapshotSummaryBuilder builder;

  auto file_a = CreateDataFile("/path/to/data-a.parquet", 500, 50);
  auto file_b = CreateDataFile("/path/to/data-b.parquet", 1000, 100);

  ASSERT_THAT(builder.AddedFile(*spec_, file_a), IsOk());
  ASSERT_THAT(builder.DeletedFile(*spec_, file_b), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "50");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "500");
  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedDataFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedRecords], "100");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedFileSize], "1000");
}

// Test position delete files
TEST_F(SnapshotSummaryBuilderTest, AddPositionDeleteFile) {
  SnapshotSummaryBuilder builder;

  auto delete_file = CreatePosDeleteFile("/path/to/deletes.parquet", 100, 10);
  ASSERT_THAT(builder.AddedFile(*spec_, delete_file), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedPosDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedPosDeletes], "10");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "100");
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kAddedDataFiles), 0);
}

// Test equality delete files
TEST_F(SnapshotSummaryBuilderTest, AddEqualityDeleteFile) {
  SnapshotSummaryBuilder builder;

  auto delete_file = CreateEqDeleteFile("/path/to/eq-deletes.parquet", 100, 10);
  ASSERT_THAT(builder.AddedFile(*spec_, delete_file), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedEqDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedEqDeletes], "10");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "100");
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kAddedPosDeleteFiles), 0);
}

// Test deletion vectors
TEST_F(SnapshotSummaryBuilderTest, AddDeletionVector) {
  SnapshotSummaryBuilder builder;

  auto dv = CreateDeletionVector("/path/to/dv.puffin", 50, 5);
  ASSERT_THAT(builder.AddedFile(*spec_, dv), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDVs], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedPosDeletes], "5");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "50");
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kAddedPosDeleteFiles), 0);
}

// Test removing deletion vectors
TEST_F(SnapshotSummaryBuilderTest, RemoveDeletionVector) {
  SnapshotSummaryBuilder builder;

  auto dv = CreateDeletionVector("/path/to/dv.puffin", 50, 5);
  ASSERT_THAT(builder.DeletedFile(*spec_, dv), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedDVs], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedPosDeletes], "5");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedFileSize], "50");
}

// Test duplicate delete tracking
TEST_F(SnapshotSummaryBuilderTest, DuplicateDeletes) {
  SnapshotSummaryBuilder builder;

  builder.IncrementDuplicateDeletes(3);

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedDuplicatedFiles], "3");
}

// Test custom properties
TEST_F(SnapshotSummaryBuilderTest, CustomProperties) {
  SnapshotSummaryBuilder builder;

  builder.Set("custom-key", "custom-value");
  builder.Set("another-key", "another-value");

  auto summary = builder.Build();

  EXPECT_EQ(summary["custom-key"], "custom-value");
  EXPECT_EQ(summary["another-key"], "another-value");
}

// Test merging builders
TEST_F(SnapshotSummaryBuilderTest, MergeBuilders) {
  SnapshotSummaryBuilder builder1;
  SnapshotSummaryBuilder builder2;

  auto file1 = CreateDataFile("/path/to/data1.parquet", 500, 50);
  auto file2 = CreateDataFile("/path/to/data2.parquet", 1000, 100);

  ASSERT_THAT(builder1.AddedFile(*spec_, file1), IsOk());
  ASSERT_THAT(builder2.AddedFile(*spec_, file2), IsOk());

  builder1.Set("custom-prop", "value1");
  builder2.Set("custom-prop", "value2");  // Should override

  builder1.Merge(builder2);

  auto summary = builder1.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "2");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "150");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "1500");
  EXPECT_EQ(summary["custom-prop"], "value2");  // Should be overridden
}

// Test manifest addition
TEST_F(SnapshotSummaryBuilderTest, AddManifest) {
  SnapshotSummaryBuilder builder;

  // First add a file directly
  auto file = CreateDataFile("/path/to/data.parquet", 1000, 100);
  ASSERT_THAT(builder.AddedFile(*spec_, file), IsOk());

  // Create a manifest
  ManifestFile manifest;
  manifest.content = ManifestContent::kData;
  manifest.added_files_count = 5;
  manifest.added_rows_count = 500;
  manifest.deleted_files_count = 2;
  manifest.deleted_rows_count = 200;

  // Add manifest (this should clear partition metrics)
  builder.AddedManifest(manifest);

  auto summary = builder.Build();

  // Should have totals from both file and manifest
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "6");  // 1 + 5
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "600");  // 100 + 500
  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedDataFiles], "2");
  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedRecords], "200");

  // Should not have partition count after adding manifest
  EXPECT_EQ(summary.count(SnapshotSummaryFields::kChangedPartitionCountProp), 0);
}

// Test data and delete files together
TEST_F(SnapshotSummaryBuilderTest, MixedDataAndDeleteFiles) {
  SnapshotSummaryBuilder builder;

  auto data_file = CreateDataFile("/path/to/data.parquet", 1000, 100);
  auto pos_delete = CreatePosDeleteFile("/path/to/pos-delete.parquet", 100, 10);
  auto eq_delete = CreateEqDeleteFile("/path/to/eq-delete.parquet", 200, 20);

  ASSERT_THAT(builder.AddedFile(*spec_, data_file), IsOk());
  ASSERT_THAT(builder.AddedFile(*spec_, pos_delete), IsOk());
  ASSERT_THAT(builder.AddedFile(*spec_, eq_delete), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "100");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDeleteFiles], "2");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedPosDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedEqDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedPosDeletes], "10");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedEqDeletes], "20");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "1300");
}

// Test multiple files of the same type
TEST_F(SnapshotSummaryBuilderTest, MultipleFiles) {
  SnapshotSummaryBuilder builder;

  for (int i = 0; i < 10; ++i) {
    auto file =
        CreateDataFile("/path/to/data-" + std::to_string(i) + ".parquet", 100, 10);
    ASSERT_THAT(builder.AddedFile(*spec_, file), IsOk());
  }

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "10");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "100");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedFileSize], "1000");
}

// Test removing multiple delete files
TEST_F(SnapshotSummaryBuilderTest, RemoveMultipleDeleteFiles) {
  SnapshotSummaryBuilder builder;

  auto pos_delete1 = CreatePosDeleteFile("/path/to/pos1.parquet", 100, 10);
  auto pos_delete2 = CreatePosDeleteFile("/path/to/pos2.parquet", 150, 15);
  auto eq_delete = CreateEqDeleteFile("/path/to/eq.parquet", 200, 20);

  ASSERT_THAT(builder.DeletedFile(*spec_, pos_delete1), IsOk());
  ASSERT_THAT(builder.DeletedFile(*spec_, pos_delete2), IsOk());
  ASSERT_THAT(builder.DeletedFile(*spec_, eq_delete), IsOk());

  auto summary = builder.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedDeleteFiles], "3");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedPosDeleteFiles], "2");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedEqDeleteFiles], "1");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedPosDeletes], "25");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedEqDeletes], "20");
  EXPECT_EQ(summary[SnapshotSummaryFields::kRemovedFileSize], "450");
}

// Test partition summary limit
TEST_F(SnapshotSummaryBuilderTest, PartitionSummaryLimit) {
  SnapshotSummaryBuilder builder;
  builder.SetPartitionSummaryLimit(2);

  auto file1 = CreateDataFile("/path/to/data1.parquet", 100, 10);
  auto file2 = CreateDataFile("/path/to/data2.parquet", 200, 20);
  auto file3 = CreateDataFile("/path/to/data3.parquet", 300, 30);

  ASSERT_THAT(builder.AddedFile(*spec_, file1), IsOk());
  ASSERT_THAT(builder.AddedFile(*spec_, file2), IsOk());
  ASSERT_THAT(builder.AddedFile(*spec_, file3), IsOk());

  auto summary = builder.Build();

  // With unpartitioned spec, all files go to the same partition ("")
  // So we should have changed partition count
  EXPECT_EQ(summary[SnapshotSummaryFields::kChangedPartitionCountProp], "1");

  // Partition summaries should be included because 1 <= 2
  EXPECT_EQ(summary[SnapshotSummaryFields::kPartitionSummaryProp], "true");
}

// Test merge with duplicates
TEST_F(SnapshotSummaryBuilderTest, MergeWithDuplicates) {
  SnapshotSummaryBuilder builder1;
  SnapshotSummaryBuilder builder2;

  builder1.IncrementDuplicateDeletes(5);
  builder2.IncrementDuplicateDeletes(3);

  auto file1 = CreateDataFile("/path/to/data1.parquet", 500, 50);
  auto file2 = CreateDataFile("/path/to/data2.parquet", 1000, 100);

  ASSERT_THAT(builder1.AddedFile(*spec_, file1), IsOk());
  ASSERT_THAT(builder2.AddedFile(*spec_, file2), IsOk());

  builder1.Merge(builder2);

  auto summary = builder1.Build();

  EXPECT_EQ(summary[SnapshotSummaryFields::kDeletedDuplicatedFiles], "8");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedDataFiles], "2");
  EXPECT_EQ(summary[SnapshotSummaryFields::kAddedRecords], "150");
}

// Test only custom properties
TEST_F(SnapshotSummaryBuilderTest, OnlyCustomProperties) {
  SnapshotSummaryBuilder builder;

  builder.Set("operation", "append");
  builder.Set("engine-name", "iceberg-cpp");

  auto summary = builder.Build();

  EXPECT_EQ(summary["operation"], "append");
  EXPECT_EQ(summary["engine-name"], "iceberg-cpp");
}

}  // namespace iceberg
