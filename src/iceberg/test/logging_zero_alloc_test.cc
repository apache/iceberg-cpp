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

// Proves the format -> dispatch path allocates no heap memory after the per-thread
// FormatBuffer warms up. Lives in its own test binary because it replaces the
// global operator new/delete with an allocation counter; the counter is only armed
// inside the measured window so the rest of the program is unaffected.

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"
#include "iceberg/test/logging_test_helpers.h"

namespace {

std::atomic<bool> g_armed{false};
std::atomic<std::size_t> g_allocs{0};

void Count() noexcept {
  if (g_armed.load(std::memory_order_relaxed)) {
    g_allocs.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace

// Global allocation-function replacement (program-wide, single definition).
void* operator new(std::size_t n) {
  Count();
  void* p = std::malloc(n != 0 ? n : 1);
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void* operator new[](std::size_t n) {
  Count();
  void* p = std::malloc(n != 0 ? n : 1);
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace iceberg {

namespace {

/// \brief Synchronous sink that consumes the record without allocating: it reads
/// the message view's size and discards it.
class CountingSink : public Logger {
 public:
  bool ShouldLog(LogLevel /*level*/) const noexcept override { return true; }
  void Log(const LogRecord& record) noexcept override { total_ += record.message.size(); }
  void SetLevel(LogLevel /*level*/) noexcept override {}
  LogLevel level() const noexcept override { return LogLevel::kTrace; }
  std::size_t total() const { return total_; }

 private:
  std::size_t total_ = 0;
};

}  // namespace

// After the thread-local FormatBuffer has grown, formatting + dispatching a
// same-size message must perform zero heap allocations.
TEST(LoggingZeroAllocTest, FormatDispatchPathDoesNotAllocateAfterWarmup) {
  auto sink = std::make_shared<CountingSink>();
  ScopedDefaultLogger guard(sink);
  const std::string payload(256, 'x');  // well past SSO

  // Warm up: first format grows FormatBuffer to >= payload size (this allocates).
  Log(LogLevel::kInfo, "{}", payload);

  g_allocs.store(0, std::memory_order_relaxed);
  g_armed.store(true, std::memory_order_relaxed);
  Log(LogLevel::kInfo, "{}", payload);  // reuses the buffer -> no allocation
  g_armed.store(false, std::memory_order_relaxed);

  EXPECT_EQ(g_allocs.load(std::memory_order_relaxed), 0u);
  EXPECT_GT(sink->total(), 0u);  // the record actually reached the sink
}

// Negative control: the counter actually observes heap allocations, so the zero
// above is meaningful and not a dead probe.
TEST(LoggingZeroAllocTest, CounterDetectsAllocation) {
  g_allocs.store(0, std::memory_order_relaxed);
  g_armed.store(true, std::memory_order_relaxed);
  std::string forced(256, 'y');  // a fresh >SSO string must hit the heap
  volatile char escape = forced[0];
  (void)escape;
  g_armed.store(false, std::memory_order_relaxed);

  EXPECT_GT(g_allocs.load(std::memory_order_relaxed), 0u);
}

}  // namespace iceberg
