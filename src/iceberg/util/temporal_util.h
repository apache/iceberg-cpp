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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT TemporalUtils {
 public:
  /// \brief Extract a date or timestamp year, as years from 1970
  static Result<Literal> ExtractYear(const Literal& literal);

  /// \brief Extract a date or timestamp month, as months from 1970-01-01
  static Result<Literal> ExtractMonth(const Literal& literal);

  /// \brief Extract a date or timestamp day, as days from 1970-01-01
  static Result<Literal> ExtractDay(const Literal& literal);

  /// \brief Extract a timestamp hour, as hours from 1970-01-01 00:00:00
  static Result<Literal> ExtractHour(const Literal& literal);
};

}  // namespace iceberg
