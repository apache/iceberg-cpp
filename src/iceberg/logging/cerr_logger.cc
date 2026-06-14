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
#include <cstdint>
#include <format>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

#if defined(_WIN32)
#  include <windows.h>
#elif defined(__APPLE__)
#  include <pthread.h>
#else
#  include <unistd.h>

#  include <sys/syscall.h>
#endif

namespace iceberg {

namespace {

/// \brief OS-native thread id, cached per thread to avoid a syscall per log.
///
/// Matches the cross-process-correlatable id used by spdlog/glog (not the opaque
/// std::thread::id), and avoids the std::formatter<std::thread::id> (P2693)
/// minimum-toolchain dependency.
uint64_t OsThreadId() noexcept {
  static thread_local uint64_t tid = []() -> uint64_t {
#if defined(_WIN32)
    return static_cast<uint64_t>(::GetCurrentThreadId());
#elif defined(__APPLE__)
    uint64_t id = 0;
    pthread_threadid_np(nullptr, &id);
    return id;
#else
    return static_cast<uint64_t>(::syscall(SYS_gettid));
#endif
  }();
  return tid;
}

/// \brief Trailing path component of a source file path.
std::string_view Basename(std::string_view path) noexcept {
  auto pos = path.find_last_of("/\\");
  return pos == std::string_view::npos ? path : path.substr(pos + 1);
}

/// \brief Format a record into a single newline-terminated line.
std::string FormatLine(const LogMessage& message) {
  auto now =
      std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
  return std::format("{:%Y-%m-%dT%H:%M:%S}Z {} [{}] {}:{}] {}\n", now,
                     ToString(message.level), OsThreadId(),
                     Basename(message.location.file_name()), message.location.line(),
                     message.message);
}

}  // namespace

void CerrLogger::Log(LogMessage&& message) noexcept {
  try {
    std::string line = FormatLine(message);
    std::lock_guard<std::mutex> lock(mutex_);
    std::cerr << line;
  } catch (...) {
    // Logging must never throw. Best-effort fallback, swallow any failure.
    try {
      std::lock_guard<std::mutex> lock(mutex_);
      std::cerr << "<log format error>\n";
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
