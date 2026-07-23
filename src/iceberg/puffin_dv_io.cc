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

#include "iceberg/puffin_dv_io.h"

#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

PuffinDVIOFactory GetNotImplementedFactory() {
  return []() -> Result<std::shared_ptr<PuffinDVIO>> {
    return NotImplemented("Missing Puffin DV I/O factory");
  };
}

}  // namespace

PuffinDVIO::~PuffinDVIO() = default;

PuffinDVIORegistry::PuffinDVIORegistry(PuffinDVIOFactory factory) {
  GetFactory() = std::move(factory);
}

PuffinDVIOFactory& PuffinDVIORegistry::GetFactory() {
  static auto* factory = new PuffinDVIOFactory(GetNotImplementedFactory());
  return *factory;
}

Result<std::vector<std::shared_ptr<DataFile>>> PuffinDVIORegistry::MergeAndWriteDVs(
    std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
    const std::shared_ptr<FileIO>& io) {
  ICEBERG_ASSIGN_OR_RAISE(auto dv_io, GetFactory()());
  ICEBERG_PRECHECK(dv_io != nullptr, "Puffin DV I/O factory returned null");
  return dv_io->MergeAndWriteDVs(groups, output_path, io);
}

}  // namespace iceberg
