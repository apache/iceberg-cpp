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

class NoopMetricsContext final : public MetricsContext {
 public:
  using MetricsContext::GetCounter;  // expose the one-arg base overload
  using MetricsContext::GetTimer;    // expose the one-arg base overload

  std::shared_ptr<Counter> GetCounter(std::string_view, CounterUnit) override {
    return Counter::Noop();
  }

  std::shared_ptr<Timer> GetTimer(std::string_view, TimerUnit) override {
    return Timer::Noop();
  }
};

}  // namespace

const std::shared_ptr<MetricsContext>& MetricsContext::Noop() {
  static std::shared_ptr<MetricsContext> instance =
      std::make_shared<NoopMetricsContext>();
  return instance;
}

std::unique_ptr<MetricsContext> MetricsContext::Default() {
  return std::make_unique<DefaultMetricsContext>();
}

std::shared_ptr<Counter> DefaultMetricsContext::GetCounter(
    [[maybe_unused]] std::string_view name, CounterUnit unit) {
  return std::make_shared<DefaultCounter>(unit);
}

std::shared_ptr<Timer> DefaultMetricsContext::GetTimer(
    [[maybe_unused]] std::string_view name, TimerUnit unit) {
  return std::make_shared<DefaultTimer>(unit);
}

}  // namespace iceberg
