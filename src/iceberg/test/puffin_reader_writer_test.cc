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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"

namespace iceberg::puffin {

namespace {

struct PuffinFile {
  MockFileIO io;
  std::string location = "memory://test.puffin";

  std::unique_ptr<OutputFile> Output() {
    auto output_file = io.NewOutputFile(location);
    EXPECT_THAT(output_file, IsOk());
    if (!output_file) {
      return nullptr;
    }
    return std::move(output_file.value());
  }

  std::unique_ptr<InputFile> Input() {
    auto input_file = io.NewInputFile(location);
    EXPECT_THAT(input_file, IsOk());
    if (!input_file) {
      return nullptr;
    }
    return std::move(input_file.value());
  }

  std::unique_ptr<InputFile> Input(int64_t file_size) {
    auto input_file = io.NewInputFile(location, static_cast<size_t>(file_size));
    EXPECT_THAT(input_file, IsOk());
    if (!input_file) {
      return nullptr;
    }
    return std::move(input_file.value());
  }

  std::vector<std::byte>& Data() { return io.FileData(location); }

  const std::vector<std::byte>& Data() const { return io.FileData(location); }

  void SetData(std::span<const std::byte> bytes) { io.AddFile(location, bytes); }
};

std::vector<std::byte> ToBytes(std::string_view str) {
  return {reinterpret_cast<const std::byte*>(str.data()),
          reinterpret_cast<const std::byte*>(str.data() + str.size())};
}

}  // namespace

TEST(PuffinWriterTest, WriteEmptyFile) {
  PuffinFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
  ASSERT_THAT(writer->Finish(), IsOk());

  auto& data = file.Data();
  EXPECT_GE(data.size(), 20u);
  EXPECT_EQ(data[0], std::byte{0x50});
  EXPECT_EQ(data[1], std::byte{0x46});
  EXPECT_EQ(data[2], std::byte{0x41});
  EXPECT_EQ(data[3], std::byte{0x31});
  auto sz = data.size();
  EXPECT_EQ(data[sz - 4], std::byte{0x50});
  EXPECT_EQ(data[sz - 3], std::byte{0x46});
  EXPECT_EQ(data[sz - 2], std::byte{0x41});
  EXPECT_EQ(data[sz - 1], std::byte{0x31});

  EXPECT_TRUE(writer->written_blobs_metadata().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto fsize, writer->FileSize());
  EXPECT_EQ(fsize, static_cast<int64_t>(data.size()));
}

TEST(PuffinWriterTest, WriterRejectsAfterFinish) {
  PuffinFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
  ASSERT_THAT(writer->Finish(), IsOk());

  EXPECT_THAT(writer->Finish(), IsError(ErrorKind::kInvalidArgument));

  Blob blob{.type = "a", .snapshot_id = 1, .sequence_number = 0};
  EXPECT_THAT(writer->Write(blob), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinWriterTest, MakeRejectsNullOutput) {
  EXPECT_THAT(PuffinWriter::Make(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinWriterTest, MakeDoesNotOverwriteExistingFile) {
  PuffinFile file;
  file.Data().push_back(std::byte{0x42});

  EXPECT_THAT(PuffinWriter::Make(file.Output()), IsError(ErrorKind::kAlreadyExists));
  ASSERT_EQ(file.Data().size(), 1);
  EXPECT_EQ(file.Data().front(), std::byte{0x42});
}

TEST(PuffinWriterTest, SizesBeforeFinishReturnError) {
  PuffinFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
  EXPECT_THAT(writer->FooterSize(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer->FileSize(), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinWriterTest, CloseImplicitlyFinishesFile) {
  PuffinFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));

  ASSERT_THAT(writer->Close(), IsOk());
  EXPECT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto fsize, writer->FileSize());
  EXPECT_EQ(fsize, static_cast<int64_t>(file.Data().size()));

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_TRUE(fm.blobs.empty());
}

TEST(PuffinRoundTripTest, SingleBlob) {
  PuffinFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer,
                           PuffinWriter::Make(file.Output(), {{"created-by", "test"}}));
    std::vector<uint8_t> blob_data = {0x01, 0x02, 0x03, 0x04, 0x05};
    ICEBERG_UNWRAP_OR_FAIL(auto meta, writer->Write(Blob{.type = "test-blob",
                                                         .input_fields = {1, 2},
                                                         .snapshot_id = 42,
                                                         .sequence_number = 7,
                                                         .data = blob_data}));
    EXPECT_EQ(meta.type, "test-blob");
    EXPECT_EQ(meta.offset, 4);
    EXPECT_EQ(meta.length, 5);
    ASSERT_THAT(writer->Finish(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto fsize, writer->FileSize());
    EXPECT_GT(fsize, 0);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  ASSERT_EQ(fm.blobs.size(), 1);
  EXPECT_EQ(fm.blobs[0].type, "test-blob");
  EXPECT_EQ(fm.properties.at("created-by"), "test");

  ICEBERG_UNWRAP_OR_FAIL(auto blob_result, reader->ReadBlob(fm.blobs[0]));
  std::vector<std::byte> expected = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                                     std::byte{0x04}, std::byte{0x05}};
  EXPECT_EQ(blob_result.second, expected);
}

TEST(PuffinRoundTripTest, MultipleBlobs) {
  PuffinFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
    ICEBERG_UNWRAP_OR_FAIL(auto m1, writer->Write(Blob{.type = "first",
                                                       .input_fields = {1},
                                                       .snapshot_id = 1,
                                                       .sequence_number = 0,
                                                       .data = {'a', 'b', 'c'}}));
    ICEBERG_UNWRAP_OR_FAIL(auto m2, writer->Write(Blob{.type = "second",
                                                       .input_fields = {2},
                                                       .snapshot_id = 2,
                                                       .sequence_number = 1,
                                                       .data = {'d', 'e', 'f', 'g'},
                                                       .properties = {{"key", "val"}}}));
    EXPECT_EQ(m2.offset, 7);
    EXPECT_EQ(m2.length, 4);
    ASSERT_THAT(writer->Finish(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  ASSERT_EQ(fm.blobs.size(), 2);
  EXPECT_TRUE(fm.blobs[0].properties.empty());
  EXPECT_EQ(fm.blobs[1].properties.at("key"), "val");

  ICEBERG_UNWRAP_OR_FAIL(auto all, reader->ReadAll(fm.blobs));
  ASSERT_EQ(all.size(), 2);
  EXPECT_EQ(all[0].second, ToBytes("abc"));
  EXPECT_EQ(all[1].second, ToBytes("defg"));
}

TEST(PuffinRoundTripTest, WithProperties) {
  PuffinFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(
        auto writer,
        PuffinWriter::Make(file.Output(), {{"created-by", "iceberg-cpp-test"}}));
    std::string text = "hello puffin";
    std::vector<uint8_t> blob_data(text.begin(), text.end());
    ASSERT_THAT(writer->Write(Blob{.type = "text-blob",
                                   .input_fields = {1},
                                   .snapshot_id = 100,
                                   .sequence_number = 5,
                                   .data = blob_data,
                                   .properties = {{"encoding", "utf-8"}}}),
                IsOk());
    ASSERT_THAT(writer->Finish(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_EQ(fm.properties.at("created-by"), "iceberg-cpp-test");
  ASSERT_EQ(fm.blobs.size(), 1);
  EXPECT_EQ(fm.blobs[0].properties.at("encoding"), "utf-8");

  ICEBERG_UNWRAP_OR_FAIL(auto blob_result, reader->ReadBlob(fm.blobs[0]));
  EXPECT_EQ(blob_result.second, ToBytes("hello puffin"));
}

TEST(PuffinReaderTest, MakeRejectsNullInput) {
  EXPECT_THAT(PuffinReader::Make(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinReaderTest, MakeUsesKnownFileSizeAndFooterSize) {
  PuffinFile file;
  int64_t footer_size = 0;
  int64_t file_size = 0;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
    ASSERT_THAT(writer->Finish(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(footer_size, writer->FooterSize());
    ICEBERG_UNWRAP_OR_FAIL(file_size, writer->FileSize());
  }

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, PuffinReader::Make(file.Input(file_size), footer_size, file_size));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_TRUE(fm.blobs.empty());
}

TEST(PuffinReaderTest, KnownFooterSizeMismatchIsRejected) {
  PuffinFile file;
  int64_t footer_size = 0;
  int64_t file_size = 0;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
    ASSERT_THAT(writer->Finish(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(footer_size, writer->FooterSize());
    ICEBERG_UNWRAP_OR_FAIL(file_size, writer->FileSize());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader,
                         PuffinReader::Make(file.Input(), footer_size - 1, file_size));
  EXPECT_THAT(reader->ReadFileMetadata(), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinReaderTest, UnknownFlagsRejected) {
  PuffinFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.Output()));
    ASSERT_THAT(writer->Finish(), IsOk());
  }
  auto& data = file.Data();
  data[data.size() - 8] = std::byte{0x02};

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  EXPECT_THAT(reader->ReadFileMetadata(), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinReaderTest, EmptyPuffinCompatibility) {
  PuffinFile file;
  std::vector<std::byte> data{
      std::byte{0x50}, std::byte{0x46}, std::byte{0x41}, std::byte{0x31}, std::byte{0x50},
      std::byte{0x46}, std::byte{0x41}, std::byte{0x31}, std::byte{0x7b}, std::byte{0x22},
      std::byte{0x62}, std::byte{0x6c}, std::byte{0x6f}, std::byte{0x62}, std::byte{0x73},
      std::byte{0x22}, std::byte{0x3a}, std::byte{0x5b}, std::byte{0x5d}, std::byte{0x7d},
      std::byte{0x0c}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x50}, std::byte{0x46},
      std::byte{0x41}, std::byte{0x31},
  };
  file.SetData(data);

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.Input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_TRUE(fm.blobs.empty());
  EXPECT_TRUE(fm.properties.empty());
}

}  // namespace iceberg::puffin
