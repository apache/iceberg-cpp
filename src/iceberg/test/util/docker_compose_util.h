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

#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "iceberg/test/util/cmd_util.h"

namespace iceberg {

/// \brief Docker Compose orchestration utilities for integration testing
class DockerCompose {
 public:
  /// \brief Initializes the Docker Compose manager context.
  /// \param project_name A unique identifier for this project to ensure test isolation.
  /// \param docker_compose_dir The directory path containing the target
  /// docker-compose.yml.
  DockerCompose(std::string project_name, std::filesystem::path docker_compose_dir);

  ~DockerCompose();

  DockerCompose(const DockerCompose&) = delete;
  DockerCompose& operator=(const DockerCompose&) = delete;
  DockerCompose(DockerCompose&&) = default;
  DockerCompose& operator=(DockerCompose&&) = default;

  /// \brief Get the docker project name.
  const std::string& project_name() const { return project_name_; }

  /// \brief Executes 'docker-compose up' to start services.
  /// \note May throw an exception if the services fail to start.
  void Up();

  /// \brief Executes 'docker-compose down' to stop and remove services.
  /// \note May throw an exception if the services fail to stop.
  void Down();

 private:
  std::string project_name_;
  std::filesystem::path docker_compose_dir_;

  /// \brief Build a docker compose Command with proper environment.
  /// \param args Additional command line arguments.
  /// \return Command object ready to execute.
  Command BuildDockerCommand(const std::vector<std::string>& args) const;
};

}  // namespace iceberg
