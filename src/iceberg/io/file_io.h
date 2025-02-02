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

namespace iceberg {
namespace io {

class ICEBERG_EXPORT InputFile {
 public:
  explicit InputFile(std::string path) : path_(std::move(path)) {}

  virtual ~InputFile() = default;

  virtual bool exists() const = 0;

  const std::string& path() const { return path_; }

 protected:
  std::string path_;
};

class ICEBERG_EXPORT OutputFile {
 public:
  explicit OutputFile(std::string path) : path_(std::move(path)) {}

  virtual ~OutputFile() = default;

  virtual void create() = 0;

  const std::string& path() const { return path_; }

 protected:
  std::string path_;
};

class ICEBERG_EXPORT FileIO {
 public:
  explicit FileIO(std::string name) : name_(std::move(name)) {}

  virtual ~FileIO() = default;

  virtual std::shared_ptr<InputFile> newInputFile(const std::string& path) = 0;
  virtual std::shared_ptr<OutputFile> newOutputFile(const std::string& path) = 0;

  virtual void DeleteFile(const std::string& path) = 0;
  void DeleteFile(const InputFile& ifile) { return DeleteFile(ifile.path()); }
  void DeleteFile(const OutputFile& ofile) { return DeleteFile(ofile.path()); }

  const std::string& name() const { return name_; }

 protected:
  std::string name_;
};

}  // namespace io
}  // namespace iceberg
