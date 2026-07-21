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

#include "iceberg/logging/cerr_logger.h"

#include <chrono>
#include <format>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

#include "iceberg/util/thread_util_internal.h"

namespace iceberg {

namespace {

/// \brief Trailing path component of a source file path.
std::string_view Basename(std::string_view path) noexcept {
  auto pos = path.find_last_of("/\\");
  return pos == std::string_view::npos ? path : path.substr(pos + 1);
}

/// \brief Format a record into a single newline-terminated line.
std::string FormatLine(const LogRecord& record) {
  auto now =
      std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
  return std::format("{:%Y-%m-%dT%H:%M:%S}Z {} [{}] [{}:{}] {}\n", now,
                     ToString(record.level), OsThreadId(),
                     Basename(record.location.file_name()), record.location.line(),
                     record.message);
}

}  // namespace

void CerrLogger::Log(const LogRecord& record) noexcept {
  try {
    std::string line = FormatLine(record);
    std::lock_guard<std::mutex> lock(mutex_);
    std::cerr << line;
  } catch (...) {
    // Logging must never throw. Reached if either formatting or the write fails;
    // emit a short marker best-effort and swallow anything further.
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      std::cerr << "<fmt error>\n";
    } catch (...) {
    }
  }
}

void CerrLogger::Flush() noexcept {
  try {
    std::lock_guard<std::mutex> lock(mutex_);
    std::cerr.flush();
  } catch (...) {
  }
}

}  // namespace iceberg
