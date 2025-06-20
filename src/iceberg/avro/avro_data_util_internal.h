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

#include <arrow/array/builder_base.h>
#include <avro/GenericDatum.hh>

#include "iceberg/schema_util.h"

namespace iceberg::avro {

/// \brief Append an Avro datum to an Arrow array builder.
///
/// This function handles schema evolution by using the provided projection to map
/// fields from the Avro data to the expected Arrow schema.
///
/// \param avro_node The Avro schema node (must be a record at root level)
/// \param avro_datum The Avro data to append
/// \param projection Schema projection from `projected_schema` to `avro_node`
/// \param projected_schema The projected schema
/// \param array_builder The Arrow array builder to append to (must be a struct builder)
/// \return Status indicating success or failure
Status AppendDatumToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const SchemaProjection& projection,
                            const Schema& projected_schema,
                            ::arrow::ArrayBuilder* array_builder);

}  // namespace iceberg::avro
