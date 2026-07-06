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

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg {

namespace {

std::string ForeignSchemeUri(std::string local_path) {
  std::ranges::replace(local_path, '\\', '/');
  return "x-store://" + local_path;
}

struct CloseState {
  bool closed = false;
};

class ReadFailureInputStream : public SeekableInputStream {
 public:
  explicit ReadFailureInputStream(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  Result<int64_t> Position() const override { return 0; }

  Status Seek(int64_t /*position*/) override { return {}; }

  Result<int64_t> Read(std::span<std::byte> /*out*/) override { return 0; }

  Status ReadFully(int64_t /*position*/, std::span<std::byte> /*out*/) override {
    return IOError("read failed");
  }

  Status Close() override {
    state_->closed = true;
    return IOError("close failed");
  }

 private:
  std::shared_ptr<CloseState> state_;
};

class ReadFailureInputFile : public InputFile {
 public:
  explicit ReadFailureInputFile(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "read-failure"; }

  Result<int64_t> Size() const override { return 4; }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    return std::make_unique<ReadFailureInputStream>(state_);
  }

 private:
  std::shared_ptr<CloseState> state_;
};

class ReadFailureFileIO : public FileIO {
 public:
  explicit ReadFailureFileIO(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(
      std::string /*file_location*/) override {
    return std::make_unique<ReadFailureInputFile>(state_);
  }

 private:
  std::shared_ptr<CloseState> state_;
};

class WriteFailureOutputStream : public PositionOutputStream {
 public:
  explicit WriteFailureOutputStream(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  Result<int64_t> Position() const override { return 0; }

  Status Write(std::span<const std::byte> /*data*/) override {
    return IOError("write failed");
  }

  Status Flush() override { return {}; }

  Status Close() override {
    state_->closed = true;
    return IOError("close failed");
  }

 private:
  std::shared_ptr<CloseState> state_;
};

class WriteFailureOutputFile : public OutputFile {
 public:
  explicit WriteFailureOutputFile(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "write-failure"; }

  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return std::make_unique<WriteFailureOutputStream>(state_);
  }

  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return std::make_unique<WriteFailureOutputStream>(state_);
  }

 private:
  std::shared_ptr<CloseState> state_;
};

class WriteFailureFileIO : public FileIO {
 public:
  explicit WriteFailureFileIO(std::shared_ptr<CloseState> state)
      : state_(std::move(state)) {}

  Result<std::unique_ptr<OutputFile>> NewOutputFile(
      std::string /*file_location*/) override {
    return std::make_unique<WriteFailureOutputFile>(state_);
  }

 private:
  std::shared_ptr<CloseState> state_;
};

struct PermissiveReadState {
  std::string data;
  bool closed = false;
  int64_t position = 0;
};

class PermissiveInputStream : public SeekableInputStream {
 public:
  explicit PermissiveInputStream(std::shared_ptr<PermissiveReadState> state)
      : state_(std::move(state)) {}

  Result<int64_t> Position() const override { return state_->position; }

  Status Seek(int64_t position) override {
    if (position < 0) {
      return InvalidArgument("Cannot seek to negative position {}", position);
    }
    state_->position = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    auto position = static_cast<size_t>(state_->position);
    if (position >= state_->data.size()) {
      return 0;
    }
    auto bytes_to_read = std::min(out.size(), state_->data.size() - position);
    std::copy_n(reinterpret_cast<const std::byte*>(state_->data.data() + position),
                bytes_to_read, out.data());
    state_->position += static_cast<int64_t>(bytes_to_read);
    return static_cast<int64_t>(bytes_to_read);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    if (position < 0) {
      return InvalidArgument("Cannot read from negative position {}", position);
    }
    auto offset = static_cast<size_t>(position);
    if (offset > state_->data.size() || out.size() > state_->data.size() - offset) {
      return IOError("Unexpected EOF");
    }
    std::copy_n(reinterpret_cast<const std::byte*>(state_->data.data() + offset),
                out.size(), out.data());
    return {};
  }

  Status Close() override {
    state_->closed = true;
    return {};
  }

 private:
  std::shared_ptr<PermissiveReadState> state_;
};

class PermissiveInputFile : public InputFile {
 public:
  explicit PermissiveInputFile(std::shared_ptr<PermissiveReadState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "permissive-input"; }

  Result<int64_t> Size() const override {
    return static_cast<int64_t>(state_->data.size());
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    return std::make_unique<PermissiveInputStream>(state_);
  }

 private:
  std::shared_ptr<PermissiveReadState> state_;
};

class PermissiveInputFileIO : public FileIO {
 public:
  explicit PermissiveInputFileIO(std::shared_ptr<PermissiveReadState> state)
      : state_(std::move(state)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(
      std::string /*file_location*/) override {
    return std::make_unique<PermissiveInputFile>(state_);
  }

 private:
  std::shared_ptr<PermissiveReadState> state_;
};

struct PermissiveWriteState {
  std::string data;
  bool closed = false;
};

class PermissiveOutputStream : public PositionOutputStream {
 public:
  explicit PermissiveOutputStream(std::shared_ptr<PermissiveWriteState> state)
      : state_(std::move(state)) {}

  Result<int64_t> Position() const override {
    return static_cast<int64_t>(state_->data.size());
  }

  Status Write(std::span<const std::byte> data) override {
    state_->data.append(reinterpret_cast<const char*>(data.data()), data.size());
    return {};
  }

  Status Flush() override { return {}; }

  Status Close() override {
    state_->closed = true;
    return {};
  }

 private:
  std::shared_ptr<PermissiveWriteState> state_;
};

class PermissiveOutputFile : public OutputFile {
 public:
  explicit PermissiveOutputFile(std::shared_ptr<PermissiveWriteState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "permissive-output"; }

  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return std::make_unique<PermissiveOutputStream>(state_);
  }

  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return std::make_unique<PermissiveOutputStream>(state_);
  }

 private:
  std::shared_ptr<PermissiveWriteState> state_;
};

class PermissiveOutputFileIO : public FileIO {
 public:
  explicit PermissiveOutputFileIO(std::shared_ptr<PermissiveWriteState> state)
      : state_(std::move(state)) {}

  Result<std::unique_ptr<OutputFile>> NewOutputFile(
      std::string /*file_location*/) override {
    return std::make_unique<PermissiveOutputFile>(state_);
  }

 private:
  std::shared_ptr<PermissiveWriteState> state_;
};

}  // namespace

class LocalFileIOTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePath();
  }

  std::shared_ptr<iceberg::FileIO> file_io_;
  std::string temp_filepath_;
};

TEST_F(LocalFileIOTest, ReadWriteFile) {
  auto read_res = file_io_->ReadFile(temp_filepath_, std::nullopt);
  EXPECT_THAT(read_res, IsError(ErrorKind::kIOError));
  EXPECT_THAT(read_res, HasErrorMessage("Failed to open local file"));

  auto write_res = file_io_->WriteFile(temp_filepath_, "hello world");
  EXPECT_THAT(write_res, IsOk());

  read_res = file_io_->ReadFile(temp_filepath_, std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello world")));
}

TEST_F(LocalFileIOTest, DeleteFile) {
  auto write_res = file_io_->WriteFile(temp_filepath_, "hello world");
  EXPECT_THAT(write_res, IsOk());

  auto del_res = file_io_->DeleteFile(temp_filepath_);
  EXPECT_THAT(del_res, IsOk());

  del_res = file_io_->DeleteFile(temp_filepath_);
  EXPECT_THAT(del_res, IsError(ErrorKind::kIOError));
  EXPECT_THAT(del_res, HasErrorMessage("Cannot delete file"));
}

TEST_F(LocalFileIOTest, DeleteFiles) {
  auto first_path = CreateNewTempFilePath();
  auto second_path = CreateNewTempFilePath();
  ASSERT_THAT(file_io_->WriteFile(first_path, "hello"), IsOk());
  ASSERT_THAT(file_io_->WriteFile(second_path, "world"), IsOk());

  std::vector<std::string> paths = {first_path, second_path};
  EXPECT_THAT(file_io_->DeleteFiles(paths), IsOk());

  EXPECT_THAT(file_io_->ReadFile(first_path, std::nullopt), IsError(ErrorKind::kIOError));
  EXPECT_THAT(file_io_->ReadFile(second_path, std::nullopt),
              IsError(ErrorKind::kIOError));
}

void VerifyReadFullyReadsFromAbsolutePosition(const std::shared_ptr<FileIO>& file_io,
                                              const std::string& path) {
  ASSERT_THAT(file_io->WriteFile(path, "abcdef"), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto input_file, file_io->NewInputFile(path));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, input_file->Open());
  ASSERT_THAT(stream->Seek(5), IsOk());

  std::array<std::byte, 2> buffer;
  ASSERT_THAT(stream->ReadFully(1, buffer), IsOk());

  std::string data(reinterpret_cast<const char*>(buffer.data()), buffer.size());
  EXPECT_EQ(data, "bc");

  ASSERT_THAT(stream->Seek(5), IsOk());
  std::array<std::byte, 1> next;
  ICEBERG_UNWRAP_OR_FAIL(auto bytes_read, stream->Read(next));
  ASSERT_EQ(bytes_read, 1);
  EXPECT_EQ(next[0], std::byte{'f'});
}

TEST_F(LocalFileIOTest, ReadFullyReadsFromAbsolutePosition) {
  ASSERT_NO_FATAL_FAILURE(
      VerifyReadFullyReadsFromAbsolutePosition(file_io_, temp_filepath_));
}

TEST_F(LocalFileIOTest, StdReadFullyReadsFromAbsolutePosition) {
  auto file_io = std::make_shared<test::StdFileIO>();
  ASSERT_NO_FATAL_FAILURE(
      VerifyReadFullyReadsFromAbsolutePosition(file_io, temp_filepath_));
}

TEST_F(LocalFileIOTest, StdReadKeepsPositionAvailableAtEof) {
  auto file_io = std::make_shared<test::StdFileIO>();
  ASSERT_THAT(file_io->WriteFile(temp_filepath_, "abc"), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto input_file, file_io->NewInputFile(temp_filepath_));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, input_file->Open());

  std::array<std::byte, 8> buffer;
  ICEBERG_UNWRAP_OR_FAIL(auto bytes_read, stream->Read(buffer));
  EXPECT_EQ(bytes_read, 3);
  EXPECT_THAT(stream->Position(), HasValue(::testing::Eq(3)));

  ICEBERG_UNWRAP_OR_FAIL(bytes_read, stream->Read(buffer));
  EXPECT_EQ(bytes_read, 0);
  EXPECT_THAT(stream->Position(), HasValue(::testing::Eq(3)));
}

TEST(ArrowFileIOTest, OutputStoredLengthAfterClose) {
  auto file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  ICEBERG_UNWRAP_OR_FAIL(auto output_file, file_io->NewOutputFile("output"));
  ICEBERG_UNWRAP_OR_FAIL(auto output, output_file->Create());

  std::array<std::byte, 3> data = {std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  ASSERT_THAT(output->Write(data), IsOk());
  ASSERT_THAT(output->Close(), IsOk());

  auto position = output->Position();
  ASSERT_FALSE(position.has_value());
  EXPECT_THAT(position.error().message, ::testing::HasSubstr("closed"));
  EXPECT_THAT(output->StoredLength(), HasValue(::testing::Eq(3)));
}

TEST_F(LocalFileIOTest, ResolvesForeignSchemeToUnderlyingPath) {
  ASSERT_THAT(file_io_->WriteFile(temp_filepath_, "hello world"), IsOk());

  auto read_res = file_io_->ReadFile(ForeignSchemeUri(temp_filepath_), std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello world")));

  auto with_query = file_io_->ReadFile(ForeignSchemeUri(temp_filepath_) + "?versionId=42",
                                       std::nullopt);
  EXPECT_THAT(with_query, IsOk());
  EXPECT_THAT(with_query, HasValue(::testing::Eq("hello world")));
}

TEST_F(LocalFileIOTest, PropagatesNonSchemeMismatchUriError) {
  auto read_res = file_io_->ReadFile("file:///tmp/%ZZ", std::nullopt);
  EXPECT_THAT(read_res, IsError(ErrorKind::kUnknownError));
  EXPECT_THAT(read_res, HasErrorMessage("Cannot parse URI"));
}

// Regression tests for #341: file: URIs with fewer than 3 slashes must be
// recognized as URIs and normalized before being passed to Arrow.
TEST_F(LocalFileIOTest, ResolvesFileUriSingleSlash) {
  ASSERT_THAT(file_io_->WriteFile(temp_filepath_, "hello"), IsOk());
  // Build a cross-platform file:/path URI: normalize backslashes to forward slashes
  // and ensure a leading '/' (Linux paths already have one; Windows drive letters don't).
  std::string p = temp_filepath_;
  std::ranges::replace(p, '\\', '/');
  std::string single_slash_uri = "file:" + (p.front() == '/' ? p : "/" + p);
  auto read_res = file_io_->ReadFile(single_slash_uri, std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello")));
}

TEST_F(LocalFileIOTest, ResolvesFileUriTripleSlash) {
  ASSERT_THAT(file_io_->WriteFile(temp_filepath_, "hello"), IsOk());
  // Build a cross-platform file:///path URI: normalize backslashes to forward slashes
  // and ensure exactly one leading '/' before the (possibly drive-lettered) path so
  // that "file://" + "/" + path produces the canonical triple-slash form on all
  // platforms. Linux: file:///tmp/...  Windows: file:///C:/Users/...
  std::string p = temp_filepath_;
  std::ranges::replace(p, '\\', '/');
  std::string triple_slash_uri = "file://" + (p.front() == '/' ? p : "/" + p);
  auto read_res = file_io_->ReadFile(triple_slash_uri, std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello")));
}

TEST_F(LocalFileIOTest, FileUriWithAuthorityPassesThrough) {
  // Non-regression: "file://host/path" is an authority-bearing URI and must NOT
  // be rewritten to "file:///host/path" (which would change its meaning).
  // Arrow's PathFromUri handles authority-bearing file URIs correctly.
  auto read_res = file_io_->ReadFile("file://remotehost/share/file.avro", std::nullopt);
  // The read will fail (no such host/path) but with an IO error from Arrow,
  // NOT a mangled path like "/remotehost/share/file.avro" on the local filesystem.
  EXPECT_THAT(read_res, IsError(ErrorKind::kIOError));
}

TEST_F(LocalFileIOTest, BareWindowsDriveLetterNotMisparsedAsUri) {
  // Verify that C:/... is NOT treated as a URI scheme (single char before ':').
  // If it were misparsed as scheme "C", ReadFile would attempt URI resolution and
  // fail with an Arrow URI error instead of a filesystem "not found" error.
  auto read_res = file_io_->ReadFile("C:/Users/test/warehouse/file.avro", std::nullopt);
  EXPECT_THAT(read_res, IsError(ErrorKind::kIOError));
  EXPECT_THAT(read_res, HasErrorMessage("Failed to open local file"));
}

TEST_F(LocalFileIOTest, FallbackDecodesPercentEncodingInKey) {
  std::string decoded_path = temp_filepath_ + " x";
  ASSERT_THAT(file_io_->WriteFile(decoded_path, "raw"), IsOk());

  auto read_res =
      file_io_->ReadFile(ForeignSchemeUri(temp_filepath_ + "%20x"), std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("raw")));
}

TEST(FileIOAdapterTest, InputAdapterRejectsReadsAfterClose) {
  auto state = std::make_shared<PermissiveReadState>();
  state->data = "abc";
  auto file_io = std::make_shared<PermissiveInputFileIO>(state);

  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(file_io, "input"));
  ASSERT_TRUE(input->Close().ok());
  ASSERT_TRUE(input->Close().ok());
  ASSERT_TRUE(state->closed);

  std::array<std::byte, 1> out;
  auto result = input->Read(static_cast<int64_t>(out.size()), out.data());
  auto read_at_result = input->ReadAt(0, static_cast<int64_t>(out.size()), out.data());

  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().ToString(), ::testing::HasSubstr("closed"));
  EXPECT_FALSE(read_at_result.ok());
  EXPECT_THAT(read_at_result.status().ToString(), ::testing::HasSubstr("closed"));
  EXPECT_EQ(state->position, 0);
}

TEST(FileIOAdapterTest, InputAdapterRejectsReadAtBeyondKnownSize) {
  auto state = std::make_shared<PermissiveReadState>();
  state->data = "abc";
  auto file_io = std::make_shared<PermissiveInputFileIO>(state);

  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(file_io, "input"));

  std::array<std::byte, 1> out;
  auto read_at_end = input->ReadAt(3, static_cast<int64_t>(out.size()), out.data());
  auto read_past_end = input->ReadAt(4, static_cast<int64_t>(out.size()), out.data());

  ASSERT_TRUE(read_at_end.ok());
  EXPECT_EQ(read_at_end.ValueOrDie(), 0);
  EXPECT_FALSE(read_past_end.ok());
  EXPECT_THAT(read_past_end.status().ToString(), ::testing::HasSubstr("out of bounds"));
}

TEST(FileIOAdapterTest, InputAdapterUsesInputFileSizeWithLengthHint) {
  auto state = std::make_shared<PermissiveReadState>();
  state->data = "abc";
  auto file_io = std::make_shared<PermissiveInputFileIO>(state);

  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(file_io, "input", 99));
  auto size = input->GetSize();

  ASSERT_TRUE(size.ok()) << size.status().ToString();
  EXPECT_EQ(size.ValueOrDie(), 3);
}

TEST(FileIOAdapterTest, OutputAdapterRejectsWritesAfterClose) {
  auto state = std::make_shared<PermissiveWriteState>();
  auto file_io = std::make_shared<PermissiveOutputFileIO>(state);

  ICEBERG_UNWRAP_OR_FAIL(auto output, arrow::OpenArrowOutputStream(file_io, "output"));
  ASSERT_TRUE(output->Close().ok());
  ASSERT_TRUE(output->Close().ok());
  ASSERT_TRUE(state->closed);

  auto status = output->Write("x", 1);
  auto flush_status = output->Flush();

  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("closed"));
  EXPECT_FALSE(flush_status.ok());
  EXPECT_THAT(flush_status.ToString(), ::testing::HasSubstr("closed"));
  EXPECT_TRUE(state->data.empty());
}

TEST(FileIOTest, ReadFileReturnsReadErrorWithCloseContext) {
  auto state = std::make_shared<CloseState>();
  ReadFailureFileIO file_io(state);

  auto result = file_io.ReadFile("read-failure", std::nullopt);

  EXPECT_TRUE(state->closed);
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
  EXPECT_THAT(result, HasErrorMessage("read failed"));
  EXPECT_THAT(result, HasErrorMessage("close failed"));
}

TEST(FileIOTest, WriteFileReturnsWriteErrorWithCloseContext) {
  auto state = std::make_shared<CloseState>();
  WriteFailureFileIO file_io(state);

  auto result = file_io.WriteFile("write-failure", "data");

  EXPECT_TRUE(state->closed);
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
  EXPECT_THAT(result, HasErrorMessage("write failed"));
  EXPECT_THAT(result, HasErrorMessage("close failed"));
}

}  // namespace iceberg
