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

#include <filesystem>
#include <map>
#include <string>
#include <vector>

/// \file iceberg/test/util/cmd_util.h
/// Utilities for building and executing shell commands in tests.

namespace iceberg {

/// \brief A shell command builder and executor for tests.
class Command {
 public:
  explicit Command(std::string program);

  /// \brief Add a single argument
  Command& Arg(std::string arg);

  /// \brief Add multiple arguments at once
  Command& Args(const std::vector<std::string>& args);

  /// \brief Set the current working directory for the command
  Command& CurrentDir(const std::filesystem::path& path);

  /// \brief Set an environment variable for the command
  Command& Env(const std::string& key, const std::string& val);

  /// \brief Execute the command and print logs
  /// \return A Status indicating success or failure
  void RunCommand(const std::string& desc) const;

 private:
  std::string program_;
  std::vector<std::string> args_;
  std::filesystem::path cwd_;
  std::map<std::string, std::string> env_vars_;

  /// \brief Format arguments for logging
  std::string FormatArgs() const;
};

}  // namespace iceberg
