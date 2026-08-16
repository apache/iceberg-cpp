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

#include "iceberg/file_io_registry.h"

#include <algorithm>
#include <mutex>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

struct RegistryState {
  struct Entry {
    std::string name;
    FileIORegistry::Factory factory;
  };

  std::mutex mutex;
  std::vector<Entry> registrations;
};

RegistryState& State() {
  static RegistryState state;
  return state;
}

// Copy entries so user callbacks run outside the registry lock.
std::vector<RegistryState::Entry> SnapshotEntries() {
  auto& state = State();
  std::lock_guard lock(state.mutex);
  return state.registrations;
}

std::string FormatNames(const std::vector<RegistryState::Entry>& entries) {
  std::string result;
  for (const auto& entry : entries) {
    if (!result.empty()) {
      result += ", ";
    }
    result += entry.name;
  }
  return result.empty() ? "(none registered)" : result;
}

}  // namespace

void FileIORegistry::Register(std::string name, Factory factory) {
  auto& state = State();
  std::lock_guard lock(state.mutex);
  std::erase_if(state.registrations, [&name](const RegistryState::Entry& entry) {
    return entry.name == name;
  });
  state.registrations.emplace_back(std::move(name), std::move(factory));
}

Result<std::unique_ptr<FileIO>> FileIORegistry::Load(std::string_view name,
                                                     const Properties& properties) {
  Factory::CreateFunction create;
  {
    auto& state = State();
    std::lock_guard lock(state.mutex);
    const auto entry =
        std::ranges::find(state.registrations, name, &RegistryState::Entry::name);
    if (entry == state.registrations.end()) {
      return NotFound("FileIO not found: {}", name);
    }
    create = entry->factory.create;
  }
  if (!create) {
    return InvalidArgument("FileIO '{}' has no create function", name);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, create(properties));
  if (!io) {
    return InvalidArgument("FileIO '{}' returned a null instance", name);
  }
  return std::move(io);
}

Result<std::string> FileIORegistry::Resolve(std::string_view scheme) {
  const std::string normalized_scheme = StringUtils::ToLower(scheme);
  const auto entries = SnapshotEntries();
  // Newest registrations take precedence.
  for (const auto& entry : std::ranges::reverse_view(entries)) {
    if (entry.factory.accepts && entry.factory.accepts(normalized_scheme)) {
      return entry.name;
    }
  }
  return NotSupported("No FileIO registered for URI scheme '{}'; registered: {}",
                      normalized_scheme, FormatNames(entries));
}

}  // namespace iceberg
