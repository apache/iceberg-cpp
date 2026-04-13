// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg/file_io_registry.h"

#include <mutex>

namespace iceberg {

namespace {

std::mutex& RegistryMutex() {
  static std::mutex mutex;
  return mutex;
}

auto& RegistryMap() {
  static std::unordered_map<std::string, FileIORegistry::Factory> registry;
  return registry;
}

}  // namespace

void FileIORegistry::Register(const std::string& name, Factory factory) {
  std::lock_guard lock(RegistryMutex());
  RegistryMap()[name] = std::move(factory);
}

Result<std::unique_ptr<FileIO>> FileIORegistry::Load(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& properties) {
  Factory factory;
  {
    std::lock_guard lock(RegistryMutex());
    auto it = RegistryMap().find(name);
    if (it == RegistryMap().end()) {
      return std::unexpected<Error>(
          {.kind = ErrorKind::kNotFound,
           .message = "FileIO implementation not found: " + name});
    }
    factory = it->second;
  }
  // Invoke factory outside the lock to avoid blocking other
  // Register/Load calls and to prevent deadlocks if the factory
  // calls back into the registry.
  return factory(properties);
}

}  // namespace iceberg
