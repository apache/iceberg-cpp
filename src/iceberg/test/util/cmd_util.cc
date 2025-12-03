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

#include "iceberg/test/util/cmd_util.h"

#include <unistd.h>

#include <array>
#include <cstring>
#include <format>
#include <iostream>
#include <print>
#include <stdexcept>

#include <sys/wait.h>

namespace iceberg {

Command::Command(std::string program) : program_(std::move(program)) {}

Command& Command::Arg(std::string arg) {
  args_.push_back(std::move(arg));
  return *this;
}

Command& Command::Args(const std::vector<std::string>& args) {
  args_.insert(args_.end(), args.begin(), args.end());
  return *this;
}

Command& Command::CurrentDir(const std::filesystem::path& path) {
  cwd_ = path;
  return *this;
}

Command& Command::Env(const std::string& key, const std::string& val) {
  env_vars_[key] = val;
  return *this;
}

void Command::RunCommand(const std::string& desc) const {
  std::println("[INFO] Starting to {}, command: {} {}", desc, program_, FormatArgs());

  std::cout.flush();
  std::cerr.flush();

  pid_t pid = fork();

  if (pid == -1) {
    std::println(stderr, "[ERROR] Fork failed: {}", std::strerror(errno));
    throw std::runtime_error(std::format("Fork failed: {}", std::strerror(errno)));
  }

  // --- Child Process ---
  if (pid == 0) {
    if (!cwd_.empty()) {
      std::error_code ec;
      std::filesystem::current_path(cwd_, ec);
      if (ec) {
        std::println(stderr, "Failed to change directory to '{}': {}", cwd_.string(),
                     ec.message());
        _exit(126);  // Command invoked cannot execute
      }
    }

    for (const auto& [k, v] : env_vars_) {
      setenv(k.c_str(), v.c_str(), 1);
    }

    std::vector<char*> argv;
    argv.reserve(args_.size() + 2);
    argv.push_back(const_cast<char*>(program_.c_str()));

    for (const auto& arg : args_) {
      argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    execvp(program_.c_str(), argv.data());

    std::println(stderr, "execvp failed: {}", std::strerror(errno));
    _exit(127);
  }

  // --- Parent Process ---
  int status = 0;
  if (waitpid(pid, &status, 0) == -1) {
    std::println(stderr, "[ERROR] waitpid failed: {}", std::strerror(errno));
    throw std::runtime_error(std::format("waitpid failed: {}", std::strerror(errno)));
  }

  int exit_code = -1;
  if (WIFEXITED(status)) {
    exit_code = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    exit_code = 128 + WTERMSIG(status);
  }

  if (exit_code == 0) {
    std::println("[INFO] {} succeed!", desc);
    return;
  } else {
    std::println(stderr, "[ERROR] {} failed. Exit code: {}", desc, exit_code);
    throw std::runtime_error(
        std::format("{} failed with exit code: {}", desc, exit_code));
  }
}

std::string Command::FormatArgs() const {
  std::string s;
  for (const auto& a : args_) {
    s += a + " ";
  }
  return s;
}

}  // namespace iceberg
