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

#include <memory>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class Catalog;

/// \brief Factory interface for creating context-bound catalog views.
///
/// `SessionCatalog` is not a replacement for `Catalog` and does not duplicate
/// table or namespace operations. It is a small root interface for catalogs that
/// can bind the standard `Catalog` API to a `SessionContext`.
///
/// Call `AsCatalog()` for the catalog's default session, or `WithContext()` when
/// the caller has an explicit session identity, properties, or credentials.
class ICEBERG_EXPORT SessionCatalog {
 public:
  virtual ~SessionCatalog() = default;

  /// \brief Catalog name.
  virtual std::string_view name() const = 0;

  /// \brief Return a `Catalog` view bound to the catalog's default session.
  ///
  /// Implementations may cache and return the same default catalog view on
  /// repeated calls.
  virtual Result<std::shared_ptr<Catalog>> AsCatalog() = 0;

  /// \brief Return a `Catalog` view bound to the supplied session context.
  ///
  /// Implementations should reject invalid contexts, such as an empty
  /// `SessionContext::session_id`. Returned catalog views use the normal
  /// `Catalog` interface for all table and namespace operations.
  virtual Result<std::shared_ptr<Catalog>> WithContext(SessionContext context) = 0;
};

}  // namespace iceberg
