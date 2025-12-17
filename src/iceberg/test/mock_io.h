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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_io.h"

namespace iceberg {

class MockFileIO : public FileIO {
 public:
  MockFileIO() = default;
  ~MockFileIO() override = default;

  MOCK_METHOD((Result<std::string>), ReadFile,
              (const std::string&, std::optional<size_t>), (override));

  MOCK_METHOD(Status, WriteFile, (const std::string&, std::string_view), (override));

  MOCK_METHOD(Status, DeleteFile, (const std::string&), (override));
};

}  // namespace iceberg
