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

namespace iceberg {

namespace {

// Wraps noop singletons behind shared_ptr without deleting them.
struct NoDelete {
  template <typename T>
  void operator()(T*) const noexcept {}
};

class NullMetricsContext final : public MetricsContext {
 public:
  std::shared_ptr<Counter> GetCounter(std::string_view, CounterUnit) override {
    // Static shared_ptr: control block allocated once, zero allocation per call.
    static const std::shared_ptr<Counter> kNoop{&Counter::Noop(), NoDelete{}};
    return kNoop;
  }

  std::shared_ptr<Timer> GetTimer(std::string_view) override {
    static const std::shared_ptr<Timer> kNoop{&Timer::Noop(), NoDelete{}};
    return kNoop;
  }
};

}  // namespace

MetricsContext& MetricsContext::Null() {
  static NullMetricsContext instance;
  return instance;
}

std::unique_ptr<MetricsContext> MetricsContext::Default() {
  return std::make_unique<DefaultMetricsContext>();
}

std::shared_ptr<Counter> DefaultMetricsContext::GetCounter(std::string_view name,
                                                           CounterUnit unit) {
  auto key = std::string(name);
  auto it = counters_.find(key);
  if (it != counters_.end()) return it->second;
  auto counter = std::make_shared<DefaultCounter>(unit);
  counters_.emplace(std::move(key), counter);
  return counter;
}

std::shared_ptr<Timer> DefaultMetricsContext::GetTimer(std::string_view name) {
  auto key = std::string(name);
  auto it = timers_.find(key);
  if (it != timers_.end()) return it->second;
  auto timer = std::make_shared<DefaultTimer>();
  timers_.emplace(std::move(key), timer);
  return timer;
}

}  // namespace iceberg
