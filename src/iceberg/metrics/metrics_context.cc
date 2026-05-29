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

#include "iceberg/metrics/metrics_context.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

namespace iceberg {

namespace {

class NullMetricsContext final : public MetricsContext {
 public:
  using MetricsContext::GetCounter;  // expose the one-arg base overload
  std::shared_ptr<Counter> GetCounter(std::string_view, CounterUnit) override {
    return Counter::Noop();
  }

  std::shared_ptr<Timer> GetTimer(std::string_view) override { return Timer::Noop(); }
};

}  // namespace

std::shared_ptr<MetricsContext> MetricsContext::Null() {
  static std::shared_ptr<MetricsContext> instance =
      std::make_shared<NullMetricsContext>();
  return instance;
}

std::unique_ptr<MetricsContext> MetricsContext::Default() {
  return std::make_unique<DefaultMetricsContext>();
}

std::shared_ptr<Counter> DefaultMetricsContext::GetCounter(std::string_view name,
                                                           CounterUnit unit) {
  {
    std::shared_lock lock(mtx_);
    auto it = counters_.find(name);
    if (it != counters_.end()) return it->second;
  }
  std::unique_lock lock(mtx_);
  auto it = counters_.find(name);
  if (it != counters_.end()) return it->second;
  auto counter = std::make_shared<DefaultCounter>(unit);
  counters_.emplace(name, counter);
  return counter;
}

std::shared_ptr<Timer> DefaultMetricsContext::GetTimer(std::string_view name) {
  {
    std::shared_lock lock(mtx_);
    auto it = timers_.find(name);
    if (it != timers_.end()) return it->second;
  }
  std::unique_lock lock(mtx_);
  auto it = timers_.find(name);
  if (it != timers_.end()) return it->second;
  auto timer = std::make_shared<DefaultTimer>();
  timers_.emplace(name, timer);
  return timer;
}

}  // namespace iceberg
