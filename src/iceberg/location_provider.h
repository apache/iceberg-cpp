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
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Interface for providing data file locations.
class ICEBERG_EXPORT LocationProvider {
 public:
  virtual ~LocationProvider() = default;

  /// \brief Return a fully-qualified data file location for the given filename.
  ///
  /// \param filename file name to get location
  /// \return a fully-qualified location URI for a data file
  virtual std::string NewDataLocation(std::string_view filename) = 0;

  /// \brief Return a fully-qualified data file location for the given partition and
  /// filename.
  ///
  /// \param spec partition spec
  /// \param partition a tuple of partition values matching the given spec
  /// \param filename file name
  /// \return a fully-qualified location URI for a data file
  virtual Result<std::string> NewDataLocation(const PartitionSpec& spec,
                                              const PartitionValues& partition,
                                              std::string_view filename) = 0;

  /// \brief Create a LocationProvider for the given table location and properties.
  ///
  /// \param location table location
  /// \param properties table properties
  /// \return a LocationProvider instance
  static Result<std::unique_ptr<LocationProvider>> Make(
      std::string_view location, const TableProperties& properties);
};

}  // namespace iceberg
