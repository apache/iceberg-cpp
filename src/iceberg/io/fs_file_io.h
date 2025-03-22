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

#pragma once

#include "iceberg/file_io.h"
#include "iceberg/io/iceberg_io_export.h"

namespace iceberg::io {

class ICEBERG_IO_EXPORT FsInputFile : public InputFile {
 public:
  explicit FsInputFile(std::string location) : InputFile(std::move(location)) {}
  ~FsInputFile() override = default;

  /// \brief Checks whether the file exists.
  bool exists() const override;

  /// \brief Returns the total length of the file, in bytes.
  int64_t getLength() const override;

  /// \brief Get a Reader instance to read bytes from the file.
  std::unique_ptr<Reader> newReader() override;
};

class ICEBERG_IO_EXPORT FsOutputFile : public OutputFile {
 public:
  explicit FsOutputFile(std::string location) : OutputFile(std::move(location)) {}
  ~FsOutputFile() override = default;

  /// \brief Create the file, optionally overwriting if it exists.
  ///
  /// If the file already exists, an exception is thrown.
  void create() override;

  /// \brief Get a Writer instance to write bytes to the file.
  ///
  /// Returns a unique pointer to a `FsFileWriter`.
  std::unique_ptr<Writer> newWriter() override;

  /// \brief Return an InputFile for the location of this OutputFile.
  ///
  /// Creates an FsInputFile for reading the file pointed by this OutputFile's location.
  std::shared_ptr<InputFile> toInputFile() const override {
    return std::make_shared<FsInputFile>(location_);
  }
};

/// \brief A concrete implementation of FileIO for file system.
class ICEBERG_IO_EXPORT FsFileIO : public FileIO {
 public:
  explicit FsFileIO(const std::string& name) : FileIO(name) {}

  ~FsFileIO() override = default;

  /// \brief Get an InputFile
  ///
  /// Returns a shared pointer to an FsInputFile instance, representing the file at
  /// `location`.
  std::shared_ptr<InputFile> newInputFile(const std::string& location) override;

  /// \brief Get an OutputFile
  ///
  /// Returns a shared pointer to an FsOutputFile instance, representing the file at
  /// `location`.
  std::shared_ptr<OutputFile> newOutputFile(const std::string& location) override;

  /// \brief Delete file
  ///
  /// Deletes the file at the given location using file system operations.
  void DeleteFile(const std::string& location) override;

 private:
  /// \brief Check if a file exists
  bool fileExists(const std::string& location);
};

}  // namespace iceberg::io
