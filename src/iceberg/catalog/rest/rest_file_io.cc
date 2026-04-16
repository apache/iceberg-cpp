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

#include "iceberg/catalog/rest/rest_file_io.h"

#include <string>

#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

bool IsBuiltinImpl(std::string_view io_impl) {
  return io_impl == FileIORegistry::kArrowLocalFileIO ||
         io_impl == FileIORegistry::kArrowS3FileIO;
}

}  // namespace

Result<BuiltinFileIOKind> DetectBuiltinFileIO(std::string_view location) {
  const auto pos = location.find("://");
  if (pos == std::string_view::npos) {
    return BuiltinFileIOKind::kArrowLocal;
  }

  const auto scheme = location.substr(0, pos);
  if (scheme == "file") {
    return BuiltinFileIOKind::kArrowLocal;
  }
  if (scheme == "s3") {
    return BuiltinFileIOKind::kArrowS3;
  }

  return NotSupported("URI scheme '{}' is not supported for automatic FileIO resolution",
                      scheme);
}

std::string_view BuiltinFileIOName(BuiltinFileIOKind kind) {
  switch (kind) {
    case BuiltinFileIOKind::kArrowLocal:
      return FileIORegistry::kArrowLocalFileIO;
    case BuiltinFileIOKind::kArrowS3:
      return FileIORegistry::kArrowS3FileIO;
  }
  std::unreachable();
}

Result<std::unique_ptr<FileIO>> MakeCatalogFileIO(const RestCatalogProperties& config) {
  std::string io_impl = config.Get(RestCatalogProperties::kIOImpl);
  std::string warehouse = config.Get(RestCatalogProperties::kWarehouse);

  if (io_impl.empty()) {
    if (warehouse.empty()) {
      return InvalidArgument(R"("{}" or "{}" property is required to create FileIO)",
                             RestCatalogProperties::kIOImpl.key(),
                             RestCatalogProperties::kWarehouse.key());
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
    io_impl = std::string(BuiltinFileIOName(detected_kind));
  }

  if (!warehouse.empty() && IsBuiltinImpl(io_impl)) {
    ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
    const auto detected_name = BuiltinFileIOName(detected_kind);
    if (io_impl != detected_name) {
      return InvalidArgument(
          R"("io-impl" value '{}' is incompatible with warehouse '{}')", io_impl,
          warehouse);
    }
  }

  // TODO(gangwu): Support Java-style customized FileIO creation flows instead of
  // resolving a single catalog-scoped FileIO instance only from properties.
  return FileIORegistry::Load(io_impl, config.configs());
}

}  // namespace iceberg::rest
