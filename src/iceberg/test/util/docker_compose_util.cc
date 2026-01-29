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

#include "iceberg/test/util/docker_compose_util.h"

#include <unistd.h>

#include <cctype>
#include <chrono>
#include <format>
#include <print>

#include "iceberg/test/util/cmd_util.h"

namespace iceberg {

namespace {
/// \brief Generate a unique test data directory path
std::filesystem::path GenerateTestDataDir() {
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  auto pid = getpid();
  return {std::format("/tmp/iceberg-test-{}-{}", timestamp, pid)};
}
}  // namespace

DockerCompose::DockerCompose(std::string project_name,
                             std::filesystem::path docker_compose_dir)
    : project_name_(std::move(project_name)),
      docker_compose_dir_(std::move(docker_compose_dir)),
      test_data_dir_(GenerateTestDataDir()) {
  std::filesystem::create_directories(test_data_dir_);
}

DockerCompose::~DockerCompose() { Down(); }

void DockerCompose::Up() {
  auto cmd = BuildDockerCommand({"up", "-d", "--wait", "--timeout", "60"});
  return cmd.RunCommand("docker compose up");
}

void DockerCompose::Down() {
  auto cmd = BuildDockerCommand({"down", "-v", "--remove-orphans"});
  cmd.RunCommand("docker compose down");

  // Clean up the test data directory
  if (!test_data_dir_.empty() && std::filesystem::exists(test_data_dir_)) {
    std::error_code ec;
    std::filesystem::remove_all(test_data_dir_, ec);
    if (!ec) {
      std::println("[INFO] Cleaned up test data directory: {}", test_data_dir_.string());
    }
  }
}

Command DockerCompose::BuildDockerCommand(const std::vector<std::string>& args) const {
  Command cmd("docker");
  // Set working directory
  cmd.CurrentDir(docker_compose_dir_);
  // Set the test data directory environment variable
  cmd.Env("ICEBERG_TEST_DATA_DIR", test_data_dir_.string());
  // Use 'docker compose' subcommand with project name
  cmd.Arg("compose").Arg("-p").Arg(project_name_).Args(args);
  return cmd;
}

}  // namespace iceberg
