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

#include "file_io.h"

namespace iceberg {
namespace io {

class ICEBERG_EXPORT FsInputFile : public InputFile {
 public:
  explicit FsInputFile(std::string path) : InputFile(std::move(path)) {}
  ~FsInputFile() = default;

  bool exists() const override;
};

class ICEBERG_EXPORT FsOutputFile : public OutputFile {
 public:
  explicit FsOutputFile(std::string path) : OutputFile(std::move(path)) {}
  ~FsOutputFile() = default;

  void create() override;
};

class ICEBERG_EXPORT FsFileIO : public FileIO {
 public:
  FsFileIO() : FileIO("fs") {}
  ~FsFileIO() = default;

  std::shared_ptr<InputFile> newInputFile(const std::string& path) override;
  std::shared_ptr<OutputFile> newOutputFile(const std::string& path) override;

  void DeleteFile(const std::string& path) override;
};

}  // namespace io
}  // namespace iceberg
