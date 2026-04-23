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
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/file_io.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/test/matchers.h"

namespace iceberg::puffin {

namespace {

// Simple in-memory stream implementations for testing.

class MemoryOutputStream : public PositionOutputStream {
 public:
  explicit MemoryOutputStream(std::shared_ptr<std::vector<std::byte>> buffer)
      : buffer_(std::move(buffer)) {}

  Result<int64_t> Position() const override {
    return static_cast<int64_t>(buffer_->size());
  }

  Status Write(std::span<const std::byte> data) override {
    buffer_->insert(buffer_->end(), data.begin(), data.end());
    return {};
  }

  Status Flush() override { return {}; }
  Status Close() override { return {}; }

 private:
  std::shared_ptr<std::vector<std::byte>> buffer_;
};

class MemoryInputStream : public SeekableInputStream {
 public:
  explicit MemoryInputStream(std::shared_ptr<std::vector<std::byte>> buffer)
      : buffer_(std::move(buffer)) {}

  Result<int64_t> Position() const override { return position_; }

  Status Seek(int64_t position) override {
    position_ = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    auto available = static_cast<int64_t>(buffer_->size()) - position_;
    auto to_read = std::min(static_cast<int64_t>(out.size()), available);
    if (to_read > 0) {
      std::memcpy(out.data(), buffer_->data() + position_, to_read);
      position_ += to_read;
    }
    return to_read;
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    if (position < 0 || position + static_cast<int64_t>(out.size()) >
                            static_cast<int64_t>(buffer_->size())) {
      return Invalid("ReadFully out of bounds");
    }
    std::memcpy(out.data(), buffer_->data() + position, out.size());
    return {};
  }

  Status Close() override { return {}; }

 private:
  std::shared_ptr<std::vector<std::byte>> buffer_;
  int64_t position_ = 0;
};

class MemoryOutputFile : public OutputFile {
 public:
  explicit MemoryOutputFile(std::shared_ptr<std::vector<std::byte>> buffer)
      : buffer_(std::move(buffer)) {}

  std::string_view location() const override { return "memory://test.puffin"; }

  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return CreateOrOverwrite();
  }

  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    buffer_->clear();
    return std::make_unique<MemoryOutputStream>(buffer_);
  }

 private:
  std::shared_ptr<std::vector<std::byte>> buffer_;
};

class MemoryInputFile : public InputFile {
 public:
  explicit MemoryInputFile(std::shared_ptr<std::vector<std::byte>> buffer)
      : buffer_(std::move(buffer)) {}

  std::string_view location() const override { return "memory://test.puffin"; }

  Result<int64_t> Size() const override { return static_cast<int64_t>(buffer_->size()); }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    return std::make_unique<MemoryInputStream>(buffer_);
  }

 private:
  std::shared_ptr<std::vector<std::byte>> buffer_;
};

// Helper to create a shared buffer and output/input file pair.
struct MemoryFile {
  std::shared_ptr<std::vector<std::byte>> buffer =
      std::make_shared<std::vector<std::byte>>();

  std::unique_ptr<OutputFile> output() {
    return std::make_unique<MemoryOutputFile>(buffer);
  }

  std::unique_ptr<InputFile> input() { return std::make_unique<MemoryInputFile>(buffer); }
};

std::vector<std::byte> ToBytes(std::string_view str) {
  return {reinterpret_cast<const std::byte*>(str.data()),
          reinterpret_cast<const std::byte*>(str.data() + str.size())};
}

}  // namespace

// ============================================================================
// PuffinWriter Tests
// ============================================================================

TEST(PuffinWriterTest, WriteEmptyFile) {
  MemoryFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
  ASSERT_THAT(writer->Finish(), IsOk());

  auto& data = *file.buffer;
  EXPECT_GE(data.size(), 20u);
  // Header magic
  EXPECT_EQ(data[0], std::byte{0x50});
  EXPECT_EQ(data[1], std::byte{0x46});
  EXPECT_EQ(data[2], std::byte{0x41});
  EXPECT_EQ(data[3], std::byte{0x31});
  // Footer end magic
  auto sz = data.size();
  EXPECT_EQ(data[sz - 4], std::byte{0x50});
  EXPECT_EQ(data[sz - 3], std::byte{0x46});
  EXPECT_EQ(data[sz - 2], std::byte{0x41});
  EXPECT_EQ(data[sz - 1], std::byte{0x31});

  EXPECT_TRUE(writer->written_blobs_metadata().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto fsize, writer->file_size());
  EXPECT_EQ(fsize, static_cast<int64_t>(data.size()));
}

TEST(PuffinWriterTest, WriterRejectsAfterFinish) {
  MemoryFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
  ASSERT_THAT(writer->Finish(), IsOk());

  // Double finish
  EXPECT_THAT(writer->Finish(), IsError(ErrorKind::kInvalid));

  // Write after finish
  Blob blob{.type = "a", .snapshot_id = 1, .sequence_number = 0};
  EXPECT_THAT(writer->Write(blob), IsError(ErrorKind::kInvalid));
}

TEST(PuffinWriterTest, MakeRejectsNullOutput) {
  EXPECT_THAT(PuffinWriter::Make(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinWriterTest, SizesBeforeFinishReturnError) {
  MemoryFile file;
  ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
  EXPECT_THAT(writer->footer_size(), IsError(ErrorKind::kInvalid));
  EXPECT_THAT(writer->file_size(), IsError(ErrorKind::kInvalid));
}

// ============================================================================
// Round-Trip Tests
// ============================================================================

TEST(PuffinRoundTripTest, SingleBlob) {
  MemoryFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer,
                           PuffinWriter::Make(file.output(), {{"created-by", "test"}}));
    std::vector<uint8_t> blob_data = {0x01, 0x02, 0x03, 0x04, 0x05};
    ICEBERG_UNWRAP_OR_FAIL(auto meta, writer->Write(Blob{.type = "test-blob",
                                                         .input_fields = {1, 2},
                                                         .snapshot_id = 42,
                                                         .sequence_number = 7,
                                                         .data = blob_data}));
    EXPECT_EQ(meta.type, "test-blob");
    EXPECT_EQ(meta.offset, 4);  // after header magic
    EXPECT_EQ(meta.length, 5);
    ASSERT_THAT(writer->Finish(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto fsize, writer->file_size());
    EXPECT_GT(fsize, 0);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.input()));
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
  MemoryFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
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
    EXPECT_EQ(m2.offset, 7);  // header(4) + first blob(3)
    EXPECT_EQ(m2.length, 4);
    ASSERT_THAT(writer->Finish(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.input()));
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
  MemoryFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(
        auto writer,
        PuffinWriter::Make(file.output(), {{"created-by", "iceberg-cpp-test"}}));
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

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.input()));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_EQ(fm.properties.at("created-by"), "iceberg-cpp-test");
  ASSERT_EQ(fm.blobs.size(), 1);
  EXPECT_EQ(fm.blobs[0].properties.at("encoding"), "utf-8");

  ICEBERG_UNWRAP_OR_FAIL(auto blob_result, reader->ReadBlob(fm.blobs[0]));
  EXPECT_EQ(blob_result.second, ToBytes("hello puffin"));
}

// ============================================================================
// PuffinReader Error Tests
// ============================================================================

TEST(PuffinReaderTest, MakeRejectsNullInput) {
  EXPECT_THAT(PuffinReader::Make(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST(PuffinReaderTest, InvalidMagic) {
  auto buffer = std::make_shared<std::vector<std::byte>>(16, std::byte{0x00});
  auto input = std::make_unique<MemoryInputFile>(buffer);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(std::move(input)));
  EXPECT_THAT(reader->ReadFileMetadata(), IsError(ErrorKind::kInvalid));
}

TEST(PuffinReaderTest, TruncatedFile) {
  auto buffer = std::make_shared<std::vector<std::byte>>(2, std::byte{0x50});
  auto input = std::make_unique<MemoryInputFile>(buffer);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(std::move(input)));
  EXPECT_THAT(reader->ReadFileMetadata(), IsError(ErrorKind::kInvalid));
}

TEST(PuffinReaderTest, InvalidBlobOffset) {
  MemoryFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
    ASSERT_THAT(writer->Finish(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.input()));
  BlobMetadata bad_meta{.type = "bad",
                        .snapshot_id = 1,
                        .sequence_number = 0,
                        .offset = 9999,
                        .length = 100};
  EXPECT_THAT(reader->ReadBlob(bad_meta), IsError(ErrorKind::kInvalid));
}

TEST(PuffinReaderTest, UnknownFlagsRejected) {
  // Build a valid puffin file then tamper with flags
  MemoryFile file;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(file.output()));
    ASSERT_THAT(writer->Finish(), IsOk());
  }
  // Set unknown flag bit in the footer struct (flags are at offset -8 from end)
  auto& data = *file.buffer;
  data[data.size() - 8] = std::byte{0x02};  // bit 1 is unknown

  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(file.input()));
  EXPECT_THAT(reader->ReadFileMetadata(), IsError(ErrorKind::kInvalid));
}

// ============================================================================
// Java Binary Compatibility Tests
// ============================================================================

TEST(PuffinReaderTest, JavaEmptyPuffinCompatibility) {
  auto buffer = std::make_shared<std::vector<std::byte>>(std::vector<std::byte>{
      std::byte{0x50}, std::byte{0x46}, std::byte{0x41}, std::byte{0x31}, std::byte{0x50},
      std::byte{0x46}, std::byte{0x41}, std::byte{0x31}, std::byte{0x7b}, std::byte{0x22},
      std::byte{0x62}, std::byte{0x6c}, std::byte{0x6f}, std::byte{0x62}, std::byte{0x73},
      std::byte{0x22}, std::byte{0x3a}, std::byte{0x5b}, std::byte{0x5d}, std::byte{0x7d},
      std::byte{0x0c}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x50}, std::byte{0x46},
      std::byte{0x41}, std::byte{0x31},
  });

  auto input = std::make_unique<MemoryInputFile>(buffer);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(std::move(input)));
  ICEBERG_UNWRAP_OR_FAIL(auto fm, reader->ReadFileMetadata());
  EXPECT_TRUE(fm.blobs.empty());
  EXPECT_TRUE(fm.properties.empty());
}

}  // namespace iceberg::puffin
