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

#include "iceberg/metrics/counter.h"

#include <memory>

namespace iceberg {

namespace {

class NoopCounter final : public Counter {
 public:
  using Counter::Increment;
  void Increment(int64_t) override {}
  int64_t value() const override { return 0; }
  bool IsNoop() const override { return true; }
};

}  // namespace

std::shared_ptr<Counter> Counter::Noop() {
  static std::shared_ptr<Counter> instance = std::make_shared<NoopCounter>();
  return instance;
}

DefaultCounter::DefaultCounter(CounterUnit unit) : unit_(unit) {}

void DefaultCounter::Increment(int64_t amount) {
  count_.fetch_add(amount, std::memory_order_relaxed);
}

int64_t DefaultCounter::value() const { return count_.load(std::memory_order_relaxed); }

}  // namespace iceberg
