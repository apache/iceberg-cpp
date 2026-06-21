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

#include "iceberg/logging/logger.h"

#include <atomic>
#include <memory>
#include <thread>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/test/logging_test_helpers.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(LoggerTest, NoopIsSharedImmortalAndSilent) {
  auto noop = Logger::Noop();
  ASSERT_NE(noop, nullptr);
  EXPECT_TRUE(noop->IsNoop());
  EXPECT_FALSE(noop->ShouldLog(LogLevel::kFatal));
  EXPECT_EQ(noop->level(), LogLevel::kOff);
  // Same singleton instance every call.
  EXPECT_EQ(noop.get(), Logger::Noop().get());
}

TEST(LoggerTest, DefaultLoggerIsNeverNull) { EXPECT_NE(GetDefaultLogger(), nullptr); }

TEST(LoggerTest, SetAndGetDefaultLogger) {
  auto capturing = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(capturing);
  EXPECT_EQ(GetDefaultLogger().get(), capturing.get());
  EXPECT_EQ(internal::CurrentLogger().get(), capturing.get());
}

TEST(LoggerTest, SetNullFallsBackToNoop) {
  ScopedDefaultLogger guard(std::make_shared<CapturingLogger>());
  SetDefaultLogger(nullptr);
  EXPECT_TRUE(GetDefaultLogger()->IsNoop());
}

TEST(LoggerTest, CurrentLoggerTracksSwaps) {
  auto first = std::make_shared<CapturingLogger>();
  auto second = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(first);
  EXPECT_EQ(internal::CurrentLogger().get(), first.get());
  SetDefaultLogger(second);
  // Generation bump must invalidate the thread-local cache.
  EXPECT_EQ(internal::CurrentLogger().get(), second.get());
}

TEST(LoggerTest, SetDefaultLevelUpdatesLogger) {
  auto capturing = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(capturing);
  SetDefaultLevel(LogLevel::kError);
  EXPECT_EQ(capturing->level(), LogLevel::kError);
}

// Filtering is decided by the logger's own ShouldLog (no separate cached gate),
// so lowering a logger's level out-of-band (not via SetDefaultLevel) takes effect
// immediately -- this is the regression guard for the dropped g_effective_level gate.
TEST(LoggerTest, OutOfBandLevelLoweringTakesEffect) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kError);
  ScopedDefaultLogger guard(capturing);
  EXPECT_FALSE(internal::CurrentLogger()->ShouldLog(LogLevel::kInfo));
  capturing->SetLevel(LogLevel::kTrace);  // lowered directly on the handle
  EXPECT_TRUE(internal::CurrentLogger()->ShouldLog(LogLevel::kInfo));
}

TEST(LoggerTest, ConcurrentSwapAndReadIsSafe) {
  // Stress CurrentLogger()/GetDefaultLogger() against SetDefaultLogger() swaps.
  // Run under TSan in CI; here it asserts no crash and a valid logger throughout.
  auto a = std::make_shared<CapturingLogger>();
  auto b = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(a);
  std::atomic<bool> stop{false};
  std::atomic<bool> saw_null{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 6; ++i) {
    readers.emplace_back([&stop, &saw_null] {
      // ASSERT_* doesn't propagate from non-main threads; record via a flag.
      while (!stop.load(std::memory_order_relaxed)) {
        const auto& l = internal::CurrentLogger();
        if (!l) saw_null.store(true, std::memory_order_relaxed);
        std::ignore = l->ShouldLog(LogLevel::kError);
        std::ignore = GetDefaultLogger();
      }
    });
  }
  for (int i = 0; i < 2000; ++i) SetDefaultLogger((i & 1) ? a : b);
  stop.store(true, std::memory_order_relaxed);
  for (auto& t : readers) t.join();
  EXPECT_FALSE(saw_null.load());  // CurrentLogger() is never null across swaps
}

TEST(LoggerTest, InitializeAppliesLevelProperty) {
  CapturingLogger logger;
  auto status = logger.Initialize({{std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(logger.level(), LogLevel::kError);
}

TEST(LoggerTest, InitializeRejectsInvalidLevel) {
  CapturingLogger logger;
  auto status =
      logger.Initialize({{std::string(kLevelProperty), std::string("not-a-level")}});
  ASSERT_FALSE(status.has_value());
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
}

// Logging during thread teardown (from a thread_local destructor) must not crash.
// The per-thread cache is freed at thread exit, but CurrentLogger()'s teardown
// guard (a trivially-destructible "dead" flag whose storage outlives every
// thread_local) makes it safe even when the logging statement runs from a
// thread_local destroyed AFTER the cache -- the hard case. Probe is constructed
// before CurrentLogger() is first touched, so it is destroyed last. Run under
// ASan/TSan in CI for full signal.
TEST(LoggerTest, LoggingFromThreadLocalDestructorIsSafe) {
  std::thread([] {
    struct Probe {
      ~Probe() {
        const auto& logger = internal::CurrentLogger();
        if (logger) {
          internal::Emit(*logger, LogLevel::kInfo, std::source_location::current(),
                         "from thread_local dtor");
        }
      }
    };
    static thread_local Probe probe;
    std::ignore = probe;  // construct Probe first ...
    std::ignore =
        internal::CurrentLogger();  // ... then the logger cache (destroyed first)
  }).join();
  SUCCEED();
}

// Teardown interleaved with concurrent default-logger swaps: many short-lived
// threads each log from a thread_local destructor while another thread swaps the
// default logger. Exercises the per-thread cache being freed at thread exit at
// the same time the global slot is mutated. Run under ASan/TSan in CI.
TEST(LoggerTest, ConcurrentTeardownAndSwapIsSafe) {
  auto a = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(a);
  std::atomic<bool> stop{false};
  std::thread swapper([&] {
    auto b = std::make_shared<CapturingLogger>();
    while (!stop.load(std::memory_order_relaxed)) {
      SetDefaultLogger(b);
      SetDefaultLogger(a);
    }
  });
  for (int i = 0; i < 100; ++i) {
    std::thread worker([] {
      struct Probe {
        ~Probe() {
          const auto& l = internal::CurrentLogger();
          if (l) l->ShouldLog(LogLevel::kError);
        }
      };
      static thread_local Probe probe;
      std::ignore = probe;                      // constructed before the cache
      std::ignore = internal::CurrentLogger();  // touch the cache in normal code
    });
    worker.join();  // teardown runs concurrently with the swapper
  }
  stop.store(true, std::memory_order_relaxed);
  swapper.join();
  SUCCEED();
}

// --- Per-context routing: ScopedLogger + GetCurrentLogger ---

TEST(LoggerTest, ScopedLoggerOverridesDefaultPath) {
  auto global = std::make_shared<CapturingLogger>();
  auto scoped = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  {
    ScopedLogger bind(scoped);
    EXPECT_EQ(internal::CurrentLogger().get(), scoped.get());
    Log(LogLevel::kInfo, "hi {}", 1);
  }
  EXPECT_EQ(scoped->count(), 1u);
  EXPECT_EQ(global->count(), 0u);
}

TEST(LoggerTest, ScopedLoggerRestoresOnScopeExit) {
  auto global = std::make_shared<CapturingLogger>();
  auto scoped = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  {
    ScopedLogger bind(scoped);
  }
  EXPECT_EQ(internal::CurrentLogger().get(), global.get());
  Log(LogLevel::kInfo, "back");
  EXPECT_EQ(global->count(), 1u);
  EXPECT_EQ(scoped->count(), 0u);
}

TEST(LoggerTest, ExplicitLoggerBypassesOverride) {
  auto scoped = std::make_shared<CapturingLogger>();
  auto explicit_sink = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(std::make_shared<CapturingLogger>());
  ScopedLogger bind(scoped);
  Log(*explicit_sink, LogLevel::kInfo, "e {}", 1);
  EXPECT_EQ(explicit_sink->count(), 1u);
  EXPECT_EQ(scoped->count(), 0u);
}

TEST(LoggerTest, NestedScopedLoggersRestoreInLifo) {
  auto global = std::make_shared<CapturingLogger>();
  auto x = std::make_shared<CapturingLogger>();
  auto y = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  {
    ScopedLogger a(x);
    EXPECT_EQ(internal::CurrentLogger().get(), x.get());
    {
      ScopedLogger b(y);
      EXPECT_EQ(internal::CurrentLogger().get(), y.get());
    }
    EXPECT_EQ(internal::CurrentLogger().get(), x.get());
  }
  EXPECT_EQ(internal::CurrentLogger().get(), global.get());
}

TEST(LoggerTest, ScopedLoggerNullMasksToGlobalDefault) {
  auto global = std::make_shared<CapturingLogger>();
  auto x = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  ScopedLogger a(x);
  {
    ScopedLogger mask(nullptr);
    EXPECT_EQ(internal::CurrentLogger().get(), global.get());  // not x, not Noop
    EXPECT_FALSE(internal::CurrentLogger()->IsNoop());
  }
  EXPECT_EQ(internal::CurrentLogger().get(), x.get());  // enclosing binding restored
}

TEST(LoggerTest, GetCurrentLoggerReturnsOverrideThenDefault) {
  auto global = std::make_shared<CapturingLogger>();
  auto scoped = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  EXPECT_EQ(GetCurrentLogger().get(), global.get());
  {
    ScopedLogger bind(scoped);
    EXPECT_EQ(GetCurrentLogger().get(), scoped.get());
  }
  EXPECT_EQ(GetCurrentLogger().get(), global.get());
}

TEST(LoggerTest, GetCurrentLoggerOnFreshThreadReturnsDefault) {
  auto global = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  Logger* seen = nullptr;
  std::thread([&] { seen = GetCurrentLogger().get(); }).join();  // never used a scope
  EXPECT_EQ(seen, global.get());
}

TEST(LoggerTest, ThreadPoolPropagationPattern) {
  auto global = std::make_shared<CapturingLogger>();
  auto scoped = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  ScopedLogger bind(scoped);
  auto captured = GetCurrentLogger();  // capture the effective logger at "submit"
  EXPECT_EQ(captured.get(), scoped.get());
  std::thread([captured] {
    ScopedLogger rebind(captured);  // re-bind on the worker thread
    Log(LogLevel::kInfo, "task {}", 7);
  }).join();
  EXPECT_EQ(scoped->count(), 1u);
  EXPECT_EQ(global->count(), 0u);
}

TEST(LoggerTest, WorkerOverrideDoesNotLeakAcrossTasksOnReusedThread) {
  auto global = std::make_shared<CapturingLogger>();
  auto o1 = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);
  Logger* during = nullptr;
  Logger* between = nullptr;
  std::thread([&] {
    {
      ScopedLogger b1(o1);
      during = GetCurrentLogger().get();
    }
    between = GetCurrentLogger().get();  // no scope active -> global default
  }).join();
  EXPECT_EQ(during, o1.get());
  EXPECT_EQ(between, global.get());
}

// Binding/unbinding a ScopedLogger from a thread_local destructor that runs after
// the per-thread cache was freed (Probe constructed before CurrentLogger is first
// touched) must no-op against the dead cache, never touch freed memory. Run under
// ASan/TSan in CI for full signal.
TEST(LoggerTest, ScopedLoggerBindUnbindDuringTeardownIsSafe) {
  std::thread([] {
    struct Probe {
      ~Probe() {
        ScopedLogger late(std::make_shared<CapturingLogger>());  // ctor: no-op on dead
        const auto& l = internal::CurrentLogger();               // -> Noop fallback
        if (l) l->ShouldLog(LogLevel::kError);
      }  // ~ScopedLogger: restore is a no-op on dead
    };
    static thread_local Probe probe;
    std::ignore = probe;                      // constructed first
    std::ignore = internal::CurrentLogger();  // cache created after -> freed first
  }).join();
  SUCCEED();
}

// The override path deliberately skips the generation refresh, but a swap that
// happened while an override was active must still be observed on the first call
// after the override is popped (gen is monotonic).
TEST(LoggerTest, OverrideActiveSkipsGenRefreshButSwapStillSeenAfterPop) {
  auto a = std::make_shared<CapturingLogger>();
  auto b = std::make_shared<CapturingLogger>();
  auto c = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(a);
  EXPECT_EQ(internal::CurrentLogger().get(), a.get());
  {
    ScopedLogger bind(b);
    SetDefaultLogger(c);  // bump gen while the override is active
    EXPECT_EQ(internal::CurrentLogger().get(), b.get());  // override wins
  }
  EXPECT_EQ(internal::CurrentLogger().get(), c.get());  // swap seen after pop
}

// Many short-lived threads each bind a ScopedLogger and tear down while another
// thread swaps the global default. Run under ASan/TSan in CI.
TEST(LoggerTest, ConcurrentOverrideTeardownAndSwapIsSafe) {
  auto a = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(a);
  std::atomic<bool> stop{false};
  std::thread swapper([&] {
    auto b = std::make_shared<CapturingLogger>();
    while (!stop.load(std::memory_order_relaxed)) {
      SetDefaultLogger(b);
      SetDefaultLogger(a);
    }
  });
  for (int i = 0; i < 100; ++i) {
    std::thread worker([] {
      auto local = std::make_shared<CapturingLogger>();
      ScopedLogger bind(local);
      const auto& l = internal::CurrentLogger();
      if (l) l->ShouldLog(LogLevel::kError);
    });
    worker.join();
  }
  stop.store(true, std::memory_order_relaxed);
  swapper.join();
  SUCCEED();
}

// --- Function-style API (non-macro): overloaded Log() ---

TEST(LoggerTest, LogToExplicitLoggerFormats) {
  auto sink = std::make_shared<CapturingLogger>();
  Log(*sink, LogLevel::kInfo, "x={} y={}", 1, 2);
  auto records = sink->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].level, LogLevel::kInfo);
  EXPECT_EQ(records[0].message, "x=1 y=2");
  EXPECT_NE(records[0].location.line(), 0u);  // call-site location captured
}

TEST(LoggerTest, LogRespectsLevelAndDoesNotFormatWhenDisabled) {
  auto sink = std::make_shared<CapturingLogger>();
  sink->SetLevel(LogLevel::kError);
  Log(*sink, LogLevel::kInfo, "dropped {}", 1);
  EXPECT_EQ(sink->count(), 0u);
}

TEST(LoggerTest, LogToDefaultLoggerFormatStyle) {
  auto sink = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(sink);
  Log(LogLevel::kWarn, "v={}", 7);
  auto records = sink->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].level, LogLevel::kWarn);
  EXPECT_EQ(records[0].message, "v=7");
}

// --- LogMessage::Builder (structured attributes) ---

TEST(LoggerTest, BuilderAssemblesMessageAndAttributes) {
  auto record = LogMessage::Builder(LogLevel::kInfo)
                    .Message("scan finished")
                    .Attribute("table", "db.t")
                    .Attribute("snapshot_id", "42")
                    .Build();
  EXPECT_EQ(record.level, LogLevel::kInfo);
  EXPECT_EQ(record.message, "scan finished");
  ASSERT_EQ(record.attributes.size(), 2u);
  EXPECT_EQ(record.attributes[0].key, "table");
  EXPECT_EQ(record.attributes[0].value, "db.t");
  EXPECT_EQ(record.attributes[1].key, "snapshot_id");
  EXPECT_EQ(record.attributes[1].value, "42");
}

TEST(LoggerTest, BuilderDefaultsAndEmitToSink) {
  auto sink = std::make_shared<CapturingLogger>();
  sink->Log(LogMessage::Builder(LogLevel::kError).Message("boom").Build());
  auto records = sink->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].level, LogLevel::kError);
  EXPECT_EQ(records[0].message, "boom");
  EXPECT_TRUE(records[0].attributes.empty());
  EXPECT_NE(records[0].location.line(), 0u);  // location defaulted at build site
}

TEST(LoggerTest, BuilderLocationOverride) {
  auto loc = std::source_location::current();
  auto record = LogMessage::Builder(LogLevel::kDebug).Location(loc).Build();
  EXPECT_EQ(record.location.line(), loc.line());
}

}  // namespace iceberg
