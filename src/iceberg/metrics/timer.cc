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

#include "iceberg/metrics/timer.h"

#include <memory>

namespace iceberg {

namespace {

class NoopTimer final : public Timer {
 public:
  int64_t Count() const override { return -1; }
  std::chrono::nanoseconds TotalDuration() const override {
    return std::chrono::nanoseconds{-1};
  }
  void Record(std::chrono::nanoseconds) override {}
  std::string_view Unit() const override { return "undefined"; }
  bool IsNoop() const override { return true; }
};

void RecordElapsedIfRunning(Timer* timer, std::chrono::steady_clock::time_point start,
                            bool& stopped) {
  if (stopped) return;
  stopped = true;
  if (!timer) return;
  auto end = std::chrono::steady_clock::now();
  timer->Record(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
}

}  // namespace

// --- Timer::Timed ---

Timer::Timed::Timed(Timer& timer)
    : timer_(&timer), start_(std::chrono::steady_clock::now()) {}

Timer::Timed::~Timed() { RecordElapsedIfRunning(timer_, start_, stopped_); }

Timer::Timed::Timed(Timed&& other) noexcept
    : timer_(other.timer_), start_(other.start_), stopped_(other.stopped_) {
  other.timer_ = nullptr;
  other.stopped_ = true;  // transfer recording responsibility
}

Timer::Timed& Timer::Timed::operator=(Timed&& other) noexcept {
  if (this != &other) {
    RecordElapsedIfRunning(timer_, start_, stopped_);
    timer_ = other.timer_;
    start_ = other.start_;
    stopped_ = other.stopped_;
    other.timer_ = nullptr;
    other.stopped_ = true;
  }
  return *this;
}

void Timer::Timed::Stop() {
  if (stopped_) return;
  stopped_ = true;
  if (!timer_) return;
  auto end = std::chrono::steady_clock::now();
  timer_->Record(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_));
}

// --- Timer ---

Timer::Timed Timer::Start() { return Timed(*this); }

std::shared_ptr<Timer> Timer::Noop() {
  static std::shared_ptr<Timer> instance = std::make_shared<NoopTimer>();
  return instance;
}

// --- DefaultTimer ---

DefaultTimer::DefaultTimer(TimerUnit unit) : unit_(unit) {}

int64_t DefaultTimer::Count() const { return count_.load(std::memory_order_relaxed); }

std::chrono::nanoseconds DefaultTimer::TotalDuration() const {
  return std::chrono::nanoseconds{total_nanos_.load(std::memory_order_relaxed)};
}

void DefaultTimer::Record(std::chrono::nanoseconds duration) {
  if (duration.count() < 0) return;
  count_.fetch_add(1, std::memory_order_relaxed);
  total_nanos_.fetch_add(duration.count(), std::memory_order_relaxed);
}

}  // namespace iceberg
