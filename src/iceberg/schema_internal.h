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

#include <nanoarrow/nanoarrow.h>

#include "iceberg/error.h"
#include "iceberg/expected.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

// Apache Arrow C++ uses "PARQUET:field_id" to store field IDs for Parquet.
// Here we follow a similar convention for Iceberg but we might also add
// "PARQUET:field_id" in the future once we implement a Parquet writer.
constexpr std::string_view kFieldIdKey = "ICEBERG:field_id";

/// \brief Convert an Iceberg schema to an Arrow schema.
///
/// \param[in] schema The Iceberg schema to convert.
/// \param[out] out The Arrow schema to convert to.
/// \return An error if the conversion fails.
expected<void, Error> ToArrowSchema(const Schema& schema, ArrowSchema* out);

/// \brief Convert an Arrow schema to an Iceberg schema.
///
/// \param[in] schema The Arrow schema to convert.
/// \param[in] schema_id The schema ID of the Iceberg schema.
/// \return The Iceberg schema or an error if the conversion fails.
expected<std::unique_ptr<Schema>, Error> FromArrowSchema(const ArrowSchema& schema,
                                                         int32_t schema_id);

}  // namespace iceberg
