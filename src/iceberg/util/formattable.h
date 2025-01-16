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

/// \file iceberg/util/formattable.h
/// Interface for objects that can be formatted via std::format.  The actual
/// std::formatter specialization is in iceberg/util/formatter.h to avoid
/// bringing in <format> unnecessarily.

#include <string>

#include "iceberg/iceberg_export.h"

namespace iceberg::util {

/// \brief Interface for objects that can be formatted via std::format.
///
/// You must include iceberg/util/formatter.h when calling std::format.
class ICEBERG_EXPORT Formattable {
 public:
  virtual ~Formattable() = default;

  /// \brief Get a user-readable string representation.
  virtual std::string ToString() const = 0;
};

}  // namespace iceberg::util
