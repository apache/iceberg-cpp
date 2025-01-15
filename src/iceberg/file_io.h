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

#include <memory>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/reader.h"
#include "iceberg/writer.h"

namespace iceberg {

/// \brief An interface used to read input files using Reader and AsyncReader
class ICEBERG_EXPORT InputFile {
 public:
  explicit InputFile(std::string location) : location_(std::move(location)) {}

  virtual ~InputFile() = default;

  /// \brief Checks whether the file exists.
  virtual bool exists() const = 0;
  /// \brief Returns the total length of the file, in bytes.
  virtual int64_t getLength() const = 0;

  /// \brief Get a Reader instance to read bytes from the file.
  virtual std::unique_ptr<Reader> newReader() = 0;

  /// \brief Get the file location
  const std::string& location() const { return location_; }

 protected:
  std::string location_;
};

/// \brief An interface used to write output files using Writer and AsyncWriter
class ICEBERG_EXPORT OutputFile {
 public:
  explicit OutputFile(std::string location) : location_(std::move(location)) {}

  virtual ~OutputFile() = default;

  /// \brief Create the file.
  ///
  /// If the file exists, or an error will be thrown.
  virtual void create() = 0;

  /// \brief Get a Writer instance to write bytes to the file.
  virtual std::unique_ptr<Writer> newWriter() = 0;

  /// \brief Get the file location
  const std::string& location() const { return location_; }

  /// \brief Return an InputFile for the location of this OutputFile.
  virtual std::shared_ptr<InputFile> toInputFile() const = 0;

 protected:
  std::string location_;
};

/// \brief Pluggable module for reading, writing, and deleting files.
///
/// Both table metadata files and data files can be written and read by this module.
class ICEBERG_EXPORT FileIO {
 public:
  explicit FileIO(std::string name) : name_(std::move(name)) {}

  virtual ~FileIO() = default;

  /// \brief Get an InputFile
  ///
  /// Get a InputFile instance to read bytes from the file at the given location.
  virtual std::shared_ptr<InputFile> newInputFile(const std::string& location) = 0;

  /// \brief Get an OutputFile
  ///
  /// Get a OutputFile instance to write bytes to the file at the given location.
  virtual std::shared_ptr<OutputFile> newOutputFile(const std::string& location) = 0;

  /// \brief Delete file
  ///
  /// Delete the file at the given location.
  virtual void DeleteFile(const std::string& location) = 0;
  void DeleteFile(const InputFile& ifile) { return DeleteFile(ifile.location()); }
  void DeleteFile(const OutputFile& ofile) { return DeleteFile(ofile.location()); }

  const std::string& name() const { return name_; }

 protected:
  std::string name_;
};

}  // namespace iceberg
