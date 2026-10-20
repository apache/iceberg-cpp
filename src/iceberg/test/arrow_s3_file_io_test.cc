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

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/logging/logger.h"
#include "iceberg/result.h"
#include "iceberg/storage_credential.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/logging_test_helpers.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/timepoint.h"

namespace {

std::optional<std::string> GetEnvIfSet(const char* key) {
  const char* value = std::getenv(key);
  if (value == nullptr || std::string_view(value).empty()) {
    return std::nullopt;
  }
  return std::string(value);
}

std::string MakeObjectUri(std::string_view base_uri, std::string_view object_name) {
  std::string object_uri(base_uri);
  if (!object_uri.ends_with('/')) {
    object_uri += '/';
  }
  object_uri += object_name;
  return object_uri;
}

std::unordered_map<std::string, std::string> PropertiesFromEnv() {
  std::unordered_map<std::string, std::string> properties;

  if (const auto access_key = GetEnvIfSet("AWS_ACCESS_KEY_ID")) {
    properties[std::string(iceberg::arrow::S3Properties::kAccessKeyId)] = *access_key;
  }
  if (const auto secret_key = GetEnvIfSet("AWS_SECRET_ACCESS_KEY")) {
    properties[std::string(iceberg::arrow::S3Properties::kSecretAccessKey)] = *secret_key;
  }
  if (const auto endpoint = GetEnvIfSet("ICEBERG_TEST_S3_ENDPOINT")) {
    properties[std::string(iceberg::arrow::S3Properties::kEndpoint)] = *endpoint;
  }
  if (const auto region = GetEnvIfSet("AWS_REGION")) {
    properties[std::string(iceberg::arrow::S3Properties::kClientRegion)] = *region;
  }

  return properties;
}

std::unordered_map<std::string, std::string> BadS3Credentials() {
  return {
      {std::string(iceberg::arrow::S3Properties::kAccessKeyId), "bad-access-key"},
      {std::string(iceberg::arrow::S3Properties::kSecretAccessKey), "bad-secret-key"}};
}

}  // namespace

namespace iceberg::arrow {

#if ICEBERG_S3_ENABLED
Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties);
#endif

namespace {

test::ThreadExecutor& DeleteExecutor() {
  static test::ThreadExecutor executor;
  return executor;
}

class ArrowS3FileIOTest : public ::testing::Test {
 protected:
#if ICEBERG_S3_ENABLED
  static void SetUpTestSuite() {
    // Off EC2 every S3 client build waits for this to time out. Not overwritten,
    // so a run that does want those credentials can still ask.
#  ifdef _WIN32
    if (std::getenv("AWS_EC2_METADATA_DISABLED") == nullptr) {
      _putenv_s("AWS_EC2_METADATA_DISABLED", "true");
    }
#  else
    ::setenv("AWS_EC2_METADATA_DISABLED", "true", /*overwrite=*/0);
#  endif
    ASSERT_THAT(InitializeS3FileIODeleteExecutor(&DeleteExecutor()), IsOk());
    auto io = MakeS3FileIO({});
    ASSERT_THAT(io, IsOk());
  }
#endif

  static void TearDownTestSuite() {
    auto status = FinalizeS3();
    if (!status.has_value()) {
      std::cerr << "Warning: FinalizeS3 failed: " << status.error().message << std::endl;
    }
  }

  void SetUp() override { base_uri_ = GetEnvIfSet("ICEBERG_TEST_S3_URI"); }

  std::string ObjectUri(std::string_view object_name) const {
    return MakeObjectUri(*base_uri_, object_name);
  }

  const std::string& BaseUri() const { return *base_uri_; }

  bool HasIntegrationEnv() const { return base_uri_.has_value(); }

 private:
  std::optional<std::string> base_uri_;
};

bool HasWarning(const CapturingLogger& logger, std::string_view substring = {}) {
  const auto records = logger.records();
  return std::ranges::any_of(records, [substring](const LogMessage& record) {
    return record.level == LogLevel::kWarn &&
           record.message.find(substring) != std::string::npos;
  });
}

std::string ExpiresAtMs(std::chrono::milliseconds valid_for) {
  return std::to_string(UnixMsFromTimePointMs(CurrentTimePointMs() + valid_for));
}

std::vector<StorageCredential> ExpiringCredentials(std::chrono::milliseconds valid_for,
                                                   std::string_view access_key,
                                                   std::string prefix = "s3") {
  return {{.prefix = std::move(prefix),
           .config = {{std::string(S3Properties::kAccessKeyId), std::string(access_key)},
                      {std::string(S3Properties::kSecretAccessKey), "secret"},
                      {std::string(S3Properties::kSessionToken), "token"},
                      {std::string(S3Properties::kSessionTokenExpiresAtMs),
                       ExpiresAtMs(valid_for)}}}};
}

Status CheckReadWrite(FileIO& io, const std::string& object_uri,
                      std::string_view content) {
  ICEBERG_RETURN_UNEXPECTED(io.WriteFile(object_uri, content));
  ICEBERG_ASSIGN_OR_RAISE(auto read, io.ReadFile(object_uri, std::nullopt));
  EXPECT_EQ(read, std::string(content));
  return io.DeleteFile(object_uri);
}

// Reads `length` bytes of `file` through a fresh stream.
Result<std::string> ReadAll(InputFile& file, size_t length) {
  ICEBERG_ASSIGN_OR_RAISE(auto stream, file.Open());
  std::string content(length, '\0');
  ICEBERG_RETURN_UNEXPECTED(
      stream->ReadFully(0, std::as_writable_bytes(std::span(content))));
  ICEBERG_RETURN_UNEXPECTED(stream->Close());
  return content;
}

enum class SubmitFailure { kStatus, kException };

class FailingDeleteExecutor : public Executor {
 public:
  FailingDeleteExecutor(SubmitFailure failure, std::shared_future<void> release)
      : failure_(failure), release_(std::move(release)) {}

  ~FailingDeleteExecutor() override { Wait(); }

  void Wait() {
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  Status Submit(ExecutorTask task) override {
    ++submissions_;
    if (submissions_ == 1) {
      threads_.emplace_back([this, task = std::move(task)]() mutable {
        release_.wait();
        std::move(task)();
        ++completed;
      });
    }
    if (submissions_ == 2) {
      submission_failed.set_value();
      if (failure_ == SubmitFailure::kStatus) {
        return ServiceUnavailable("executor rejected deletion");
      }
      throw std::runtime_error("executor rejected deletion");
    }
    return {};
  }

  std::promise<void> submission_failed;
  std::atomic<int> completed = 0;

 private:
  SubmitFailure failure_;
  std::shared_future<void> release_;
  int submissions_ = 0;
  std::vector<std::thread> threads_;
};

}  // namespace

TEST(DeleteExecutorStateTest, InitializationIsImmutable) {
  test::InlineExecutor executor;
  DeleteExecutorState state;
  EXPECT_THAT(state.Initialize(&executor), IsOk());
  EXPECT_THAT(state.Initialize(&executor), IsOk());
  EXPECT_THAT(state.Initialize(nullptr), IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(state.Get(), &executor);

  DeleteExecutorState sequential;
  EXPECT_EQ(sequential.Get(), nullptr);
  EXPECT_THAT(sequential.Initialize(&executor), IsError(ErrorKind::kInvalidArgument));
}

TEST(DeleteExecutorStateTest, ConcurrentInitializationChoosesOneExecutor) {
  test::InlineExecutor first;
  test::InlineExecutor second;
  DeleteExecutorState state;
  std::barrier start(2);
  std::atomic<int> initialized = 0;
  auto initialize = [&](Executor* executor) {
    start.arrive_and_wait();
    if (state.Initialize(executor).has_value()) {
      ++initialized;
    }
  };
  std::thread a(initialize, &first);
  std::thread b(initialize, &second);
  a.join();
  b.join();
  EXPECT_EQ(initialized, 1);
  EXPECT_TRUE(state.Get() == &first || state.Get() == &second);
}

TEST(BulkDeleteFilesTest, LimitsConcurrentDeletes) {
  const std::array<std::pair<size_t, size_t>, 4> cases = {
      {{2, 3}, {16, 1}, {16, 3}, {128, 3}}};
  for (const auto& [file_count, parallelism] : cases) {
    SCOPED_TRACE(file_count);
    SCOPED_TRACE(parallelism);
    const auto workers = std::min(file_count, parallelism);
    test::ThreadExecutor executor;
    std::vector<std::string> paths;
    for (size_t i = 0; i < file_count; ++i) {
      paths.push_back(std::to_string(i));
    }
    std::vector<std::atomic<int>> attempts(file_count);
    std::promise<void> started;
    auto all_started = started.get_future();
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<size_t> active = 0;
    std::atomic<size_t> peak = 0;
    std::atomic<size_t> attempted = 0;
    auto run = std::async(std::launch::async, [&] {
      return BulkDeleteFiles(
          paths, &executor, parallelism, [&](const std::string& path) -> Status {
            ++attempts.at(std::stoul(path));
            const auto running = ++active;
            auto previous = peak.load();
            while (previous < running && !peak.compare_exchange_weak(previous, running)) {
            }
            if (++attempted == workers) {
              started.set_value();
            }
            released.wait();
            --active;
            return {};
          });
    });
    const auto ready = all_started.wait_for(std::chrono::seconds(5));
    release.set_value();
    EXPECT_THAT(run.get(), IsOk());
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(peak, workers);
    EXPECT_EQ(attempted, file_count);
    EXPECT_EQ(executor.submit_count(), workers);
    for (const auto& count : attempts) {
      EXPECT_EQ(count.load(), 1);
    }
  }
}

TEST(BulkDeleteFilesTest, CountsThrownDeletesAndAttemptsRemainingFiles) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel);
    test::ThreadExecutor executor;
    const std::vector<std::string> paths = {"status", "exception", "unknown", "ok"};
    const auto caller = std::this_thread::get_id();
    std::vector<std::string> sequential_attempts;
    std::atomic<int> attempted = 0;
    auto logger = std::make_shared<CapturingLogger>();
    ScopedLogger bind(logger);
    auto status = BulkDeleteFiles(paths, parallel ? &executor : nullptr, 3,
                                  [&](const std::string& path) -> Status {
                                    if (!parallel) {
                                      EXPECT_EQ(std::this_thread::get_id(), caller);
                                      sequential_attempts.push_back(path);
                                    }
                                    ++attempted;
                                    if (path == "status") {
                                      return IOError("delete failed");
                                    }
                                    if (path == "exception") {
                                      throw std::runtime_error("delete threw");
                                    }
                                    if (path == "unknown") {
                                      throw 42;
                                    }
                                    return {};
                                  });
    EXPECT_THAT(status, HasErrorMessage("Failed to delete 3 of 4 files"));
    EXPECT_EQ(attempted, 4);
    EXPECT_EQ(logger->count(), 3);
    if (!parallel) {
      EXPECT_EQ(sequential_attempts, paths);
    }
  }
}

TEST(BulkDeleteFilesTest, DrainsAcceptedTasksWhenSubmissionFails) {
  for (auto failure : {SubmitFailure::kStatus, SubmitFailure::kException}) {
    SCOPED_TRACE(static_cast<int>(failure));
    std::promise<void> release;
    FailingDeleteExecutor executor(failure, release.get_future().share());
    auto submission_failed = executor.submission_failed.get_future();
    const std::vector<std::string> paths = {"first", "second", "third"};
    std::atomic<int> attempted = 0;
    auto run = std::async(std::launch::async, [&] {
      const auto caller = std::this_thread::get_id();
      return BulkDeleteFiles(paths, &executor, 3, [&](const std::string& path) -> Status {
        EXPECT_NE(std::this_thread::get_id(), caller);
        ++attempted;
        if (path == "second") {
          return IOError("delete failed");
        }
        return {};
      });
    });
    const auto submitted = submission_failed.wait_for(std::chrono::seconds(5));
    const auto returned = run.wait_for(std::chrono::milliseconds(100));
    // Unblock workers before assertions.
    release.set_value();
    auto status = run.get();
    executor.Wait();
    EXPECT_EQ(submitted, std::future_status::ready);
    EXPECT_EQ(returned, std::future_status::timeout);
    EXPECT_THAT(status, HasErrorMessage("executor rejected deletion"));
    EXPECT_THAT(status, HasErrorMessage("failed to delete 1 of 3 files"));
    EXPECT_THAT(status,
                IsError(failure == SubmitFailure::kStatus ? ErrorKind::kServiceUnavailable
                                                          : ErrorKind::kIOError));
    EXPECT_EQ(attempted, 3);
    EXPECT_EQ(executor.completed, 1);
  }
}

TEST(BulkDeleteFilesTest, DoesNotDeleteOnTheCallerWhenSubmissionFails) {
  test::ThreadExecutor executor(ServiceUnavailable("executor is shut down"));
  const std::vector<std::string> paths = {"first", "second", "third"};
  int attempted = 0;
  EXPECT_THAT(BulkDeleteFiles(paths, &executor, 3,
                              [&](const std::string&) -> Status {
                                ++attempted;
                                return {};
                              }),
              IsError(ErrorKind::kServiceUnavailable));
  EXPECT_EQ(attempted, 0);
  EXPECT_EQ(executor.submit_count(), 1);
}

TEST(BulkDeleteFilesTest, EmptyDeleteDoesNotSubmitTasks) {
  test::InlineExecutor executor;
  EXPECT_THAT(BulkDeleteFiles({}, &executor, 3,
                              [](const std::string&) -> Status {
                                ADD_FAILURE()
                                    << "An empty delete should not invoke the callback";
                                return {};
                              }),
              IsOk());
  EXPECT_EQ(executor.submit_count(), 0);
}

TEST_F(ArrowS3FileIOTest, Create) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST_F(ArrowS3FileIOTest, StoresCredentials) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  std::vector<StorageCredential> credentials = {
      {.prefix = "s3://bucket/table",
       .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                  {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
}

TEST_F(ArrowS3FileIOTest, SkipsNonS3CredentialPrefix) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  // A server may vend credentials for several storage systems at once.
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket/table", .config = {{"k", "v"}}},
      {.prefix = "s3://bucket/table",
       .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                  {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
  // The whole list is retained, but only the S3 one is applied — and it must
  // be, otherwise the skip silently degrades to "no credentials at all".
  EXPECT_FALSE(HasWarning(*logger));
}

// Every prefix form this FileIO serves must be accepted without the warning;
// real selection is covered by AppliesOssCredentialInRealRoundTrip.
TEST_F(ArrowS3FileIOTest, AcceptsEveryS3CompatibleCredentialPrefix) {
  for (std::string_view prefix :
       {"s3", "s3://bucket/table", "s3a://bucket/table", "s3n://bucket/table",
        "oss://bucket/table", "OSS://bucket/table"}) {
    SCOPED_TRACE(prefix);
    auto result = MakeS3FileIO({});
    ASSERT_THAT(result, IsOk());
    auto* credentialed = result.value()->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    auto logger = std::make_shared<CapturingLogger>();
    ScopedDefaultLogger scoped(logger);
    std::vector<StorageCredential> credentials = {
        {.prefix = std::string(prefix),
         .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                    {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
    EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
    EXPECT_FALSE(HasWarning(*logger));
  }
}

TEST_F(ArrowS3FileIOTest, WarnsWhenNoCredentialApplies) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  // Succeeds (S3 falls back to the default credentials) but must not be silent.
  // Bare `S3` is foreign: only URI-form prefixes match case-insensitively.
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket/table", .config = {{"k", "v"}}},
      {.prefix = "S3", .config = {{"k", "v"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
  EXPECT_TRUE(HasWarning(*logger));
}

TEST_F(ArrowS3FileIOTest, RefreshesOnlyCredentialsCloseToExpiry) {
  struct Case {
    std::string_view name;
    std::vector<StorageCredential> initial;
    bool refreshes;
  };
  const std::vector<Case> cases = {
      {.name = "close to expiry",
       .initial = ExpiringCredentials(std::chrono::minutes(1), "expiring-key"),
       .refreshes = true},
      {.name = "not close to expiry",
       .initial = ExpiringCredentials(std::chrono::hours(1), "valid-key"),
       .refreshes = false},
      {.name = "static keys",
       .initial = {{.prefix = "s3", .config = BadS3Credentials()}},
       .refreshes = false}};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
    auto* credentialed = io->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    const auto refreshed = ExpiringCredentials(std::chrono::hours(1), "refreshed-key");
    auto provider = std::make_shared<MockStorageCredentialProvider>();
    ON_CALL(*provider, Load()).WillByDefault(::testing::Return(refreshed));
    EXPECT_CALL(*provider, Load()).Times(test_case.refreshes ? 1 : 0);
    ASSERT_THAT(credentialed->InitializeStorageCredentials(test_case.initial, provider),
                IsOk());

    // The replacement is not close to expiry, so the second does not refresh.
    EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
    EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
    EXPECT_EQ(credentialed->credentials(),
              test_case.refreshes ? refreshed : test_case.initial);
  }
}

TEST_F(ArrowS3FileIOTest, KeepsProviderWhenCredentialsAreUpdated) {
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto refreshed = ExpiringCredentials(std::chrono::hours(2), "refreshed-key");
  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load()).WillOnce(::testing::Return(refreshed));
  ASSERT_THAT(credentialed->InitializeStorageCredentials(
                  ExpiringCredentials(std::chrono::hours(1), "initial-key"), provider),
              IsOk());
  ASSERT_THAT(credentialed->SetStorageCredentials(
                  ExpiringCredentials(std::chrono::minutes(1), "expiring-key")),
              IsOk());

  EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
  EXPECT_EQ(credentialed->credentials(), refreshed);
}

TEST_F(ArrowS3FileIOTest, FailedInitializationDoesNotInstallProvider) {
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto initial = ExpiringCredentials(std::chrono::minutes(1), "initial-key");
  ASSERT_THAT(credentialed->InitializeStorageCredentials(initial, nullptr), IsOk());
  auto invalid = initial;
  invalid.front().prefix.clear();
  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load()).Times(0);
  EXPECT_THAT(credentialed->InitializeStorageCredentials(invalid, provider),
              IsError(ErrorKind::kValidationFailed));

  EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
  EXPECT_EQ(credentialed->credentials(), initial);
}

TEST_F(ArrowS3FileIOTest, RefreshesOnceWhenCredentialsHaveExpired) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  std::mutex mutex;
  std::condition_variable cv;
  bool refresh_started = false;
  bool release_refresh = false;
  std::atomic<int> refresh_calls = 0;

  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load())
      .WillRepeatedly([&]() -> Result<std::vector<StorageCredential>> {
        ++refresh_calls;
        std::unique_lock lock(mutex);
        refresh_started = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_refresh; });
        return ExpiringCredentials(std::chrono::hours(1), "refreshed-key");
      });
  ASSERT_THAT(credentialed->InitializeStorageCredentials(
                  ExpiringCredentials(-std::chrono::minutes(1), "expired-key"), provider),
              IsOk());

  std::thread winner(
      [&] { EXPECT_THAT(result.value()->NewInputFile("s3://bucket/key"), IsOk()); });
  {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return refresh_started; });
  }

  bool loser_ready = false;
  std::thread loser([&] {
    {
      std::lock_guard lock(mutex);
      loser_ready = true;
    }
    cv.notify_all();
    EXPECT_THAT(result.value()->NewInputFile("s3://bucket/key"), IsOk());
  });
  {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return loser_ready; });
    release_refresh = true;
  }
  cv.notify_all();
  winner.join();
  loser.join();

  EXPECT_EQ(refresh_calls, 1);
  EXPECT_THAT(credentialed->credentials(), ::testing::Not(::testing::IsEmpty()));
}

TEST_F(ArrowS3FileIOTest, RefreshDoesNotUndoCredentialsInstalledWhileItRan) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  std::mutex mutex;
  std::condition_variable cv;
  bool refresh_started = false;
  bool release_refresh = false;

  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load())
      .WillRepeatedly([&]() -> Result<std::vector<StorageCredential>> {
        std::unique_lock lock(mutex);
        refresh_started = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_refresh; });
        return ExpiringCredentials(std::chrono::hours(1), "fetched-by-refresh");
      });
  ASSERT_THAT(credentialed->InitializeStorageCredentials(
                  ExpiringCredentials(std::chrono::minutes(1), "expiring-key"), provider),
              IsOk());

  std::thread operation(
      [&] { EXPECT_THAT(result.value()->NewInputFile("s3://bucket/key"), IsOk()); });

  const auto installed =
      ExpiringCredentials(std::chrono::hours(2), "installed-meanwhile");
  {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return refresh_started; });
  }
  auto install_status = credentialed->SetStorageCredentials(installed);
  {
    std::lock_guard lock(mutex);
    release_refresh = true;
  }
  cv.notify_all();
  operation.join();

  ASSERT_THAT(install_status, IsOk());
  EXPECT_EQ(credentialed->credentials(), installed);
}

TEST_F(ArrowS3FileIOTest, RefreshesSessionCredentialsWithoutAUsableExpiry) {
  for (std::string_view expiry : {"", "not-a-number"}) {
    SCOPED_TRACE(expiry);
    auto result = MakeS3FileIO({});
    ASSERT_THAT(result, IsOk());
    auto* credentialed = result.value()->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    int refresh_calls = 0;
    auto provider = std::make_shared<MockStorageCredentialProvider>();
    EXPECT_CALL(*provider, Load())
        .WillRepeatedly([&]() -> Result<std::vector<StorageCredential>> {
          ++refresh_calls;
          return ExpiringCredentials(std::chrono::hours(1), "refreshed-key");
        });

    auto logger = std::make_shared<CapturingLogger>();
    ScopedDefaultLogger scoped(logger);
    std::unordered_map<std::string, std::string> config = {
        {std::string(S3Properties::kAccessKeyId), "access-key"},
        {std::string(S3Properties::kSecretAccessKey), "secret"},
        {std::string(S3Properties::kSessionToken), "token"}};
    if (!expiry.empty()) {
      config[std::string(S3Properties::kSessionTokenExpiresAtMs)] = std::string(expiry);
    }
    ASSERT_THAT(credentialed->InitializeStorageCredentials(
                    {{.prefix = "s3", .config = std::move(config)}}, provider),
                IsOk());
    EXPECT_TRUE(HasWarning(*logger, "session token"));

    EXPECT_THAT(result.value()->NewInputFile("s3://bucket/key"), IsOk());
    EXPECT_EQ(refresh_calls, 1);
  }
}

TEST_F(ArrowS3FileIOTest, BacksOffWhenTheReplacementIsAlsoCloseToExpiry) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  int refresh_calls = 0;
  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load())
      .WillRepeatedly([&]() -> Result<std::vector<StorageCredential>> {
        ++refresh_calls;
        return ExpiringCredentials(std::chrono::minutes(1), "short-lived-key");
      });
  ASSERT_THAT(credentialed->InitializeStorageCredentials(
                  ExpiringCredentials(std::chrono::minutes(1), "expiring-key"), provider),
              IsOk());

  for (int i = 0; i < 3; ++i) {
    EXPECT_THAT(result.value()->NewInputFile("s3://bucket/key"), IsOk());
  }
  EXPECT_EQ(refresh_calls, 1);
}

TEST_F(ArrowS3FileIOTest, KeepsCredentialsWhenTheRefreshFails) {
  struct Case {
    std::string_view name;
    std::optional<std::string> expires_at_ms;
    std::string prefix = "s3";
    std::unordered_map<std::string, std::string> settings;
    bool keys = true;
    bool load_fails = false;
  };
  const auto valid = ExpiresAtMs(std::chrono::hours(1));
  const std::vector<Case> cases = {
      {.name = "catalog error", .expires_at_ms = valid, .load_fails = true},
      {.name = "expired", .expires_at_ms = ExpiresAtMs(-std::chrono::minutes(1))},
      {.name = "no expiry", .expires_at_ms = std::nullopt},
      {.name = "unparseable expiry", .expires_at_ms = "not-a-number"},
      // A refresh may only rotate keys.
      {.name = "prefix changed", .expires_at_ms = valid, .prefix = "s3://other/"},
      {.name = "setting changed",
       .expires_at_ms = valid,
       .settings = {{std::string(S3Properties::kConnectTimeoutMs), "5000"}}},
      {.name = "no keys", .expires_at_ms = valid, .keys = false}};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
    auto* credentialed = io->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    auto config = test_case.settings;
    if (test_case.keys) {
      config.insert({{std::string(S3Properties::kAccessKeyId), "unusable-key"},
                     {std::string(S3Properties::kSecretAccessKey), "secret"}});
    }
    config.emplace(std::string(S3Properties::kSessionToken), "token");
    if (test_case.expires_at_ms.has_value()) {
      config[std::string(S3Properties::kSessionTokenExpiresAtMs)] =
          *test_case.expires_at_ms;
    }
    const std::vector<StorageCredential> unusable = {
        {.prefix = test_case.prefix, .config = std::move(config)}};
    auto provider = std::make_shared<MockStorageCredentialProvider>();
    // Once: a failed refresh backs off.
    EXPECT_CALL(*provider, Load())
        .WillOnce([&]() -> Result<std::vector<StorageCredential>> {
          if (test_case.load_fails) {
            return NotFound("catalog unreachable");
          }
          return unusable;
        });
    const auto expiring = ExpiringCredentials(std::chrono::minutes(1), "expiring-key");
    ASSERT_THAT(credentialed->InitializeStorageCredentials(expiring, provider), IsOk());

    auto logger = std::make_shared<CapturingLogger>();
    ScopedDefaultLogger scoped(logger);
    EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
    EXPECT_EQ(credentialed->credentials(), expiring);
    EXPECT_TRUE(HasWarning(*logger, "Failed to refresh"));
    EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
  }
}

TEST_F(ArrowS3FileIOTest, RejectsARefreshWithAnInvalidCredential) {
  // Usable S3 keys do not make up for an invalid credential beside them, even
  // one this FileIO would skip.
  const std::vector<StorageCredential> invalid = {
      {.prefix = "", .config = {{"k", "v"}}},
      {.prefix = "gs://bucket", .config = {}},
      {.prefix = "s3://other/", .config = {}}};
  for (const auto& bad : invalid) {
    SCOPED_TRACE(bad.prefix);
    ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
    auto* credentialed = io->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    auto refreshed = ExpiringCredentials(std::chrono::hours(1), "refreshed-key");
    refreshed.push_back(bad);
    auto provider = std::make_shared<MockStorageCredentialProvider>();
    EXPECT_CALL(*provider, Load()).WillOnce(::testing::Return(refreshed));
    const auto expiring = ExpiringCredentials(std::chrono::minutes(1), "expiring-key");
    ASSERT_THAT(credentialed->InitializeStorageCredentials(expiring, provider), IsOk());

    auto logger = std::make_shared<CapturingLogger>();
    ScopedDefaultLogger scoped(logger);
    EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
    EXPECT_EQ(credentialed->credentials(), expiring);
    EXPECT_TRUE(HasWarning(*logger, "Failed to refresh"));
  }
}

TEST_F(ArrowS3FileIOTest, AcceptsRefreshedPrefixesInAnotherOrder) {
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  auto initial =
      ExpiringCredentials(std::chrono::minutes(1), "expiring-key", "s3://first/");
  initial.push_back(
      ExpiringCredentials(std::chrono::minutes(1), "expiring-key", "s3://second/")
          .front());
  auto refreshed =
      ExpiringCredentials(std::chrono::hours(1), "refreshed-key", "s3://second/");
  refreshed.push_back(
      ExpiringCredentials(std::chrono::hours(1), "refreshed-key", "s3://first/").front());
  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load()).WillOnce(::testing::Return(refreshed));
  ASSERT_THAT(credentialed->InitializeStorageCredentials(initial, provider), IsOk());

  EXPECT_THAT(io->NewInputFile("s3://first/key"), IsOk());
  EXPECT_THAT(credentialed->credentials(),
              ::testing::UnorderedElementsAreArray(refreshed));
}

TEST_F(ArrowS3FileIOTest, TracksTheExpiryOfTheTokenItSignsWith) {
  const std::unordered_map<std::string, std::string> expired_token = {
      {std::string(S3Properties::kSessionToken), "token"},
      {std::string(S3Properties::kSessionTokenExpiresAtMs), "0"}};
  auto keys_and = [](std::unordered_map<std::string, std::string> config) {
    config.merge(BadS3Credentials());
    return config;
  };
  struct Case {
    std::string_view name;
    std::unordered_map<std::string, std::string> properties;
    std::vector<StorageCredential> vended;
    bool expired;
  };
  const std::vector<Case> cases = {
      // Without an access key pair the AWS default chain signs instead.
      {.name = "token without keys",
       .vended = {{.prefix = "s3://bucket/", .config = expired_token}},
       .expired = false},
      {.name = "expiry without a token",
       .vended = {{.prefix = "s3://bucket/",
                   .config = keys_and(
                       {{std::string(S3Properties::kSessionTokenExpiresAtMs), "0"}})}},
       .expired = false},
      // Inherited from the FileIO's properties, it signs with the vended keys.
      {.name = "inherited token",
       .properties = expired_token,
       .vended = {{.prefix = "s3://bucket/", .config = BadS3Credentials()}},
       .expired = true},
      // Only the first credential for a prefix is used.
      {.name = "repeated prefix",
       .vended = {{.prefix = "s3://bucket/", .config = BadS3Credentials()},
                  {.prefix = "s3://bucket/", .config = keys_and(expired_token)}},
       .expired = false}};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(test_case.properties));
    auto* credentialed = io->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);
    ASSERT_THAT(credentialed->SetStorageCredentials(test_case.vended), IsOk());

    if (test_case.expired) {
      EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsError(ErrorKind::kTokenExpired));
    } else {
      EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsOk());
    }
  }
}

TEST_F(ArrowS3FileIOTest, FailsOnceCredentialsHaveExpiredAndCannotBeRefreshed) {
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO({}));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load()).WillOnce([]() -> Result<std::vector<StorageCredential>> {
    return NotFound("catalog unreachable");
  });
  ASSERT_THAT(
      credentialed->InitializeStorageCredentials(
          ExpiringCredentials(-std::chrono::minutes(1), "expired-key", "s3://bucket/"),
          provider),
      IsOk());

  EXPECT_THAT(io->NewInputFile("s3://bucket/key"), IsError(ErrorKind::kTokenExpired));
  EXPECT_THAT(io->NewOutputFile("s3://bucket/key"), IsError(ErrorKind::kTokenExpired));
  EXPECT_THAT(io->DeleteFile("s3://bucket/key"), IsError(ErrorKind::kTokenExpired));
  EXPECT_THAT(io->DeleteFiles({"s3://bucket/key"}),
              HasErrorMessage("Failed to delete 1 of 1 files"));
  // Locations outside the vended prefix keep the default credentials.
  EXPECT_THAT(io->NewInputFile("s3://other-bucket/key"), IsOk());
}

TEST_F(ArrowS3FileIOTest, OperationsSurviveConcurrentCredentialInstalls) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  auto credential = [](std::string_view access_key) {
    return StorageCredential{
        .prefix = "s3://bucket",
        .config = {{std::string(S3Properties::kAccessKeyId), std::string(access_key)},
                   {std::string(S3Properties::kSecretAccessKey), "secret"}}};
  };
  ASSERT_THAT(credentialed->SetStorageCredentials({credential("first")}), IsOk());

  std::atomic<bool> stop = false;
  std::atomic<int> started = 0;
  std::atomic<int> failures = 0;
  std::vector<std::thread> operations;
  operations.reserve(4);
  for (int i = 0; i < 4; ++i) {
    operations.emplace_back([&] {
      auto open = [&] {
        if (!result.value()->NewInputFile("s3://bucket/key").has_value()) {
          ++failures;
        }
      };
      open();
      ++started;
      while (!stop.load()) {
        open();
      }
    });
  }
  // Every worker has run and is still looping before the first install.
  while (started.load() < 4) {
    std::this_thread::yield();
  }
  // No assertions until the threads are joined: a fatal assertion here would
  // destroy joinable threads and terminate the binary, masking the failure.
  Status install_status = {};
  for (int round = 0; round < 3 && install_status.has_value(); ++round) {
    install_status = credentialed->SetStorageCredentials({credential("replacement")});
  }
  stop = true;
  for (auto& operation : operations) {
    operation.join();
  }
  ASSERT_THAT(install_status, IsOk());
  EXPECT_EQ(failures, 0);
}

TEST_F(ArrowS3FileIOTest, RejectsIncompleteStaticCredentials) {
  auto result =
      MakeS3FileIO({{std::string(S3Properties::kAccessKeyId), "access-key-only"}});
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage(
                          "S3 client access key ID and secret access key must be set"));
}

TEST_F(ArrowS3FileIOTest, SharesDeleteExecutorWithRegistryCreatedFileIOs) {
  RegisterAll();
  const auto key = std::string(S3Properties::kDeleteNumThreads);
  ICEBERG_UNWRAP_OR_FAIL(auto direct, MakeS3FileIO({{key, "1"}}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto registered,
      FileIORegistry::Load(FileIORegistry::kArrowS3FileIO, {{key, "2"}}));
  auto& executor = DeleteExecutor();
  const auto before = executor.submit_count();
  // A bucket without a key fails before network I/O.
  const std::vector<std::string> paths = {"s3://bucket", "s3://bucket"};
  EXPECT_THAT(direct->DeleteFiles(paths),
              HasErrorMessage("Failed to delete 2 of 2 files"));
  EXPECT_EQ(executor.submit_count() - before, 1);
  EXPECT_THAT(registered->DeleteFiles(paths),
              HasErrorMessage("Failed to delete 2 of 2 files"));
  EXPECT_EQ(executor.submit_count() - before, 3);
}

TEST_F(ArrowS3FileIOTest, ReadWrite) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }
  auto io_res = MakeS3FileIO();
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_test.txt");
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3"), IsOk());
}

TEST_F(ArrowS3FileIOTest, ReadWriteWithProperties) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }
  auto io_res = MakeS3FileIO(PropertiesFromEnv());
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_props_test.txt");
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3 with properties"), IsOk());
}

TEST_F(ArrowS3FileIOTest, LongestCredentialPrefix) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (properties.empty()) {
    GTEST_SKIP() << "Set S3 properties to enable credential routing test";
  }

  auto io_res = MakeS3FileIO(properties);
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  constexpr std::string_view object_name = "iceberg_s3_io_prefix_test.txt";
  auto object_uri = ObjectUri(object_name);
  const auto partial_prefix =
      object_uri.substr(0, object_uri.size() - object_name.size() + 3);

  auto bad_properties = BadS3Credentials();
  EXPECT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = BaseUri(), .config = std::move(bad_properties)},
                   {.prefix = partial_prefix, .config = properties}}),
              IsOk());
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3 with vended credentials"),
              IsOk());
}

TEST_F(ArrowS3FileIOTest, RefreshReachesHandlesOpenedBeforeIt) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  const auto access_key = properties.find(std::string(S3Properties::kAccessKeyId));
  const auto secret_key = properties.find(std::string(S3Properties::kSecretAccessKey));
  if (access_key == properties.end() || secret_key == properties.end()) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential refresh test";
  }

  // New keys reach a handle opened before them, from a refresh its own request
  // runs, but not keys installed with other client settings.
  struct Case {
    std::string_view name;
    bool refresh;
    std::unordered_map<std::string, std::string> settings;
    bool handle_reads;
  };
  const std::vector<Case> cases = {
      {.name = "refresh", .refresh = true, .settings = {}, .handle_reads = true},
      {.name = "install with another setting",
       .refresh = false,
       .settings = {{std::string(S3Properties::kConnectTimeoutMs), "5000"}},
       .handle_reads = false}};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(properties));
    auto* credentialed = io->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    const auto prefix = ObjectUri("refresh_in_place/");
    const auto uri = prefix + "object";
    constexpr std::string_view kContent = "read with keys refreshed after it was opened";
    ASSERT_THAT(io->WriteFile(uri, kContent), IsOk());

    // The vended keys are bad and come due right after the handle opens; only
    // the new ones can read.
    constexpr auto kDueIn = std::chrono::seconds(2);
    auto new_config = test_case.settings;
    new_config.insert(*access_key);
    new_config.insert(*secret_key);
    const std::vector<StorageCredential> new_credentials = {
        {.prefix = prefix, .config = std::move(new_config)}};
    std::shared_ptr<MockStorageCredentialProvider> provider;
    if (test_case.refresh) {
      provider = std::make_shared<MockStorageCredentialProvider>();
      EXPECT_CALL(*provider, Load()).WillOnce(::testing::Return(new_credentials));
    }
    ASSERT_THAT(
        credentialed->InitializeStorageCredentials(
            {{.prefix = prefix,
              .config = {{std::string(S3Properties::kAccessKeyId), "bad-access-key"},
                         {std::string(S3Properties::kSecretAccessKey), "bad-secret"},
                         {std::string(S3Properties::kSessionToken), "bad-token"},
                         {std::string(S3Properties::kSessionTokenExpiresAtMs),
                          ExpiresAtMs(std::chrono::minutes(5) + kDueIn)}}}},
            provider),
        IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto file, io->NewInputFile(uri, kContent.size()));

    if (test_case.refresh) {
      std::this_thread::sleep_for(kDueIn + std::chrono::seconds(1));
    } else {
      ASSERT_THAT(credentialed->SetStorageCredentials(new_credentials), IsOk());
    }
    auto read = ReadAll(*file, kContent.size());
    if (test_case.handle_reads) {
      EXPECT_THAT(read, HasValue(::testing::Eq(std::string(kContent))));
    } else {
      // Still signs with the bad keys it was opened with.
      EXPECT_THAT(read, ::testing::Not(IsOk()));
    }
    EXPECT_EQ(credentialed->credentials(), new_credentials);
    EXPECT_THAT(io->DeleteFile(uri), IsOk());
  }
}

TEST_F(ArrowS3FileIOTest, HandleKeepsItsKeysOnceTheFileIOIsGone) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  const auto access_key = properties.find(std::string(S3Properties::kAccessKeyId));
  const auto secret_key = properties.find(std::string(S3Properties::kSecretAccessKey));
  if (access_key == properties.end() || secret_key == properties.end()) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential install test";
  }

  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(properties));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto prefix = ObjectUri("install_then_release/");
  const auto uri = prefix + "object";
  constexpr std::string_view kContent = "read after its FileIO is gone";
  ASSERT_THAT(io->WriteFile(uri, kContent), IsOk());

  // Working keys replace bad ones with the same settings. Building their client
  // must not leave it with the bad keys once the FileIO cannot supply new ones.
  ASSERT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = prefix, .config = BadS3Credentials()}}),
              IsOk());
  ASSERT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = prefix, .config = {*access_key, *secret_key}}}),
              IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file, io->NewInputFile(uri, kContent.size()));
  io.reset();

  EXPECT_THAT(ReadAll(*file, kContent.size()),
              HasValue(::testing::Eq(std::string(kContent))));
  ICEBERG_UNWRAP_OR_FAIL(auto cleanup, MakeS3FileIO(properties));
  EXPECT_THAT(cleanup->DeleteFile(uri), IsOk());
}

TEST_F(ArrowS3FileIOTest, RequestWaitsForARefreshUnderWayOnceExpired) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  const auto access_key = properties.find(std::string(S3Properties::kAccessKeyId));
  const auto secret_key = properties.find(std::string(S3Properties::kSecretAccessKey));
  if (access_key == properties.end() || secret_key == properties.end()) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential refresh test";
  }

  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(properties));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto prefix = ObjectUri("refresh_while_expired/");
  const auto uri = prefix + "object";
  constexpr std::string_view kContent = "read with keys a refresh under way fetched";
  ASSERT_THAT(io->WriteFile(uri, kContent), IsOk());

  // Bad keys that expire right after the handle opens. The first refresh
  // fails; the second, after expiry, is slow and returns working keys.
  constexpr auto kExpiresIn = std::chrono::seconds(2);
  std::promise<void> refresh_started;
  auto started = refresh_started.get_future();
  const std::vector<StorageCredential> refreshed = {
      {.prefix = prefix, .config = {*access_key, *secret_key}}};
  auto provider = std::make_shared<MockStorageCredentialProvider>();
  EXPECT_CALL(*provider, Load())
      .WillOnce([]() -> Result<std::vector<StorageCredential>> {
        return ServiceUnavailable("catalog unavailable");
      })
      .WillOnce([&]() -> Result<std::vector<StorageCredential>> {
        refresh_started.set_value();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return refreshed;
      });
  ASSERT_THAT(
      credentialed->InitializeStorageCredentials(
          {{.prefix = prefix,
            .config = {{std::string(S3Properties::kAccessKeyId), "bad-access-key"},
                       {std::string(S3Properties::kSecretAccessKey), "bad-secret"},
                       {std::string(S3Properties::kSessionToken), "bad-token"},
                       {std::string(S3Properties::kSessionTokenExpiresAtMs),
                        ExpiresAtMs(kExpiresIn)}}}},
          provider),
      IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file, io->NewInputFile(uri, kContent.size()));

  // Past the expiry, and the backoff after the failed refresh.
  std::this_thread::sleep_for(kExpiresIn + std::chrono::milliseconds(500));
  std::thread operation([&] { EXPECT_THAT(io->NewInputFile(uri), IsOk()); });
  // Read while that refresh runs. No fatal assertions before the join.
  EXPECT_EQ(started.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  auto read = ReadAll(*file, kContent.size());
  operation.join();

  EXPECT_THAT(read, HasValue(::testing::Eq(std::string(kContent))));
  EXPECT_EQ(credentialed->credentials(), refreshed);
  EXPECT_THAT(io->DeleteFile(uri), IsOk());
}

TEST_F(ArrowS3FileIOTest, DeleteFilesAttemptsEveryFile) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (properties.empty()) {
    GTEST_SKIP() << "Set S3 properties to enable credential routing test";
  }

  auto io_res = MakeS3FileIO(properties);
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto denied = ObjectUri("delete_denied/");
  const auto allowed = ObjectUri("delete_allowed/") + "only";
  const std::vector<std::string> paths = {denied + "first", allowed, denied + "second"};
  for (const auto& path : paths) {
    ASSERT_THAT(io->WriteFile(path, "payload"), IsOk());
  }

  // Deletes under `denied` fail; the one between them must still run.
  auto bad_properties = properties;
  for (const auto& [key, value] : BadS3Credentials()) {
    bad_properties.insert_or_assign(key, value);
  }
  ASSERT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = denied, .config = std::move(bad_properties)}}),
              IsOk());
  // Each failure reaches the caller's logger, whichever thread hit it.
  auto logger = std::make_shared<CapturingLogger>();
  {
    ScopedLogger bind(logger);
    EXPECT_THAT(io->DeleteFiles(paths), HasErrorMessage("Failed to delete 2 of 3 files"));
  }
  EXPECT_EQ(std::ranges::count_if(
                logger->records(),
                [](const LogMessage& record) { return record.level == LogLevel::kWarn; }),
            2);
  EXPECT_THAT(io->ReadFile(allowed, std::nullopt),
              ::testing::AllOf(IsError(ErrorKind::kIOError),
                               HasErrorMessage("Path does not exist")));

  // The denied files remain: Arrow fails to delete a missing object.
  ASSERT_THAT(credentialed->SetStorageCredentials({}), IsOk());
  EXPECT_THAT(io->DeleteFiles({paths[0], paths[2]}), IsOk());
}

// The credential and object use the same S3-compatible scheme. The scheme is
// preserved for routing, while the underlying endpoint remains S3-compatible.
TEST_F(ArrowS3FileIOTest, AppliesOssCredentialInRealRoundTrip) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (!properties.contains(std::string(S3Properties::kAccessKeyId)) ||
      !properties.contains(std::string(S3Properties::kSecretAccessKey))) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential routing test";
  }

  auto bad_defaults = properties;
  for (const auto& [key, value] : BadS3Credentials()) {
    bad_defaults.insert_or_assign(key, value);
  }
  auto io_res = MakeS3FileIO(std::move(bad_defaults));
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  constexpr std::string_view object_name = "iceberg_oss_credential_test.txt";
  const auto object_uri = ObjectUri(object_name);
  const auto scheme_end = object_uri.find("://");
  ASSERT_NE(scheme_end, std::string::npos) << "ICEBERG_TEST_S3_URI must carry a scheme";
  // Both spellings are forced, so they cross whatever scheme the env URI uses.
  const auto oss_uri = std::string("oss").append(object_uri.substr(scheme_end));
  const auto oss_prefix = oss_uri.substr(0, oss_uri.size() - object_name.size());

  EXPECT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = oss_prefix, .config = std::move(properties)}}),
              IsOk());
  EXPECT_THAT(CheckReadWrite(*io, oss_uri, "hello oss with vended credentials"), IsOk());
}

TEST_F(ArrowS3FileIOTest, DeleteFilesReachesEveryCredentialPrefix) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (!properties.contains(std::string(S3Properties::kAccessKeyId)) ||
      !properties.contains(std::string(S3Properties::kSecretAccessKey))) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential routing test";
  }

  // Only the prefix delegates can authenticate, so a location that falls to
  // the default one fails the batch instead of passing unnoticed.
  auto bad_defaults = properties;
  for (const auto& [key, value] : BadS3Credentials()) {
    bad_defaults.insert_or_assign(key, value);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(std::move(bad_defaults)));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto a = ObjectUri("delete_files_a/");
  const auto b = ObjectUri("delete_files_b/");
  ASSERT_THAT(credentialed->SetStorageCredentials({{.prefix = a, .config = properties},
                                                   {.prefix = b, .config = properties}}),
              IsOk());

  const std::vector<std::string> paths = {a + "first", b + "only", a + "second"};
  for (const auto& path : paths) {
    ASSERT_THAT(io->WriteFile(path, "payload"), IsOk());
  }
  ASSERT_THAT(io->DeleteFiles(paths), IsOk());
  // The writes above authenticated on these paths, so a failed read means the
  // object is gone.
  for (const auto& path : paths) {
    EXPECT_FALSE(io->ReadFile(path, std::nullopt).has_value()) << path;
  }
}

TEST_F(ArrowS3FileIOTest, InputFileOutlivesCredentialInstall) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (!properties.contains(std::string(S3Properties::kAccessKeyId)) ||
      !properties.contains(std::string(S3Properties::kSecretAccessKey))) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential routing test";
  }

  auto bad_defaults = properties;
  for (const auto& [key, value] : BadS3Credentials()) {
    bad_defaults.insert_or_assign(key, value);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto io, MakeS3FileIO(std::move(bad_defaults)));
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  const auto prefix = ObjectUri("retained_input/");
  const std::vector<StorageCredential> credentials = {
      {.prefix = prefix, .config = properties}};
  ASSERT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());

  const auto uri = prefix + "object";
  constexpr std::string_view kContent = "written before the credential install";
  ASSERT_THAT(io->WriteFile(uri, kContent), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file, io->NewInputFile(uri));
  ICEBERG_UNWRAP_OR_FAIL(auto opened_before, file->Open());

  // Retires the delegate `file` came from; both handles must still read.
  ASSERT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto opened_after, file->Open());
  for (auto* stream : {opened_before.get(), opened_after.get()}) {
    std::string read(kContent.size(), '\0');
    EXPECT_THAT(stream->ReadFully(0, std::as_writable_bytes(std::span(read))), IsOk());
    EXPECT_EQ(read, kContent);
    EXPECT_THAT(stream->Close(), IsOk());
  }
  EXPECT_THAT(io->DeleteFile(uri), IsOk());
}

#if ICEBERG_S3_ENABLED
TEST_F(ArrowS3FileIOTest, ClientRegion) {
  auto result =
      ConfigureS3Options({{std::string(S3Properties::kClientRegion), "us-east-1"}});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->region, "us-east-1");
}

TEST_F(ArrowS3FileIOTest, EndpointScheme) {
  struct Case {
    std::string_view endpoint;
    std::string_view endpoint_override;
    std::string_view scheme;
  };
  const std::vector<Case> cases = {
      {"https://s3.example.com:443", "s3.example.com:443", "https"},
      {"http://localhost:9000", "localhost:9000", "http"},
      {"localhost:9000", "localhost:9000", "https"}};

  for (const auto& test_case : cases) {
    auto result = ConfigureS3Options(
        {{std::string(S3Properties::kEndpoint), std::string(test_case.endpoint)}});
    ASSERT_THAT(result, IsOk()) << test_case.endpoint;
    EXPECT_EQ(result->endpoint_override, test_case.endpoint_override);
    EXPECT_EQ(result->scheme, test_case.scheme);
  }
}

TEST_F(ArrowS3FileIOTest, SslEnabled) {
  auto https =
      ConfigureS3Options({{std::string(S3Properties::kEndpoint), "http://localhost:9000"},
                          {std::string(S3Properties::kSslEnabled), "TRUE"}});
  ASSERT_THAT(https, IsOk());
  EXPECT_EQ(https->scheme, "https");

  auto http = ConfigureS3Options(
      {{std::string(S3Properties::kEndpoint), "https://localhost:9000"},
       {std::string(S3Properties::kSslEnabled), "FaLsE"}});
  ASSERT_THAT(http, IsOk());
  EXPECT_EQ(http->scheme, "http");
}

TEST_F(ArrowS3FileIOTest, PathStyleAccess) {
  auto virtual_addressing =
      ConfigureS3Options({{std::string(S3Properties::kPathStyleAccess), "FALSE"}});
  ASSERT_THAT(virtual_addressing, IsOk());
  EXPECT_TRUE(virtual_addressing->force_virtual_addressing);

  auto path_style =
      ConfigureS3Options({{std::string(S3Properties::kPathStyleAccess), "TrUe"}});
  ASSERT_THAT(path_style, IsOk());
  EXPECT_FALSE(path_style->force_virtual_addressing);
}

TEST_F(ArrowS3FileIOTest, InvalidBooleanPropertyReadsAsFalse) {
  auto options =
      ConfigureS3Options({{std::string(S3Properties::kPathStyleAccess), "not-a-bool"}});
  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->force_virtual_addressing);
}

TEST_F(ArrowS3FileIOTest, Timeouts) {
  auto result =
      ConfigureS3Options({{std::string(S3Properties::kConnectTimeoutMs), "5000"},
                          {std::string(S3Properties::kSocketTimeoutMs), "10000"}});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->connect_timeout, 5);
  EXPECT_EQ(result->request_timeout, 10);
}
#endif

}  // namespace iceberg::arrow
