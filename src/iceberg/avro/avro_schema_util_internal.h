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

#include <stack>

#include <avro/Node.hh>

#include "iceberg/result.h"
#include "iceberg/type.h"

namespace iceberg::avro {

/// \brief A visitor that converts an Iceberg type to an Avro node.
class ToAvroNodeVisitor {
 public:
  Status Visit(const BooleanType& type, ::avro::NodePtr* node);
  Status Visit(const IntType& type, ::avro::NodePtr* node);
  Status Visit(const LongType& type, ::avro::NodePtr* node);
  Status Visit(const FloatType& type, ::avro::NodePtr* node);
  Status Visit(const DoubleType& type, ::avro::NodePtr* node);
  Status Visit(const DecimalType& type, ::avro::NodePtr* node);
  Status Visit(const DateType& type, ::avro::NodePtr* node);
  Status Visit(const TimeType& type, ::avro::NodePtr* node);
  Status Visit(const TimestampType& type, ::avro::NodePtr* node);
  Status Visit(const TimestampTzType& type, ::avro::NodePtr* node);
  Status Visit(const StringType& type, ::avro::NodePtr* node);
  Status Visit(const UuidType& type, ::avro::NodePtr* node);
  Status Visit(const FixedType& type, ::avro::NodePtr* node);
  Status Visit(const BinaryType& type, ::avro::NodePtr* node);
  Status Visit(const StructType& type, ::avro::NodePtr* node);
  Status Visit(const ListType& type, ::avro::NodePtr* node);
  Status Visit(const MapType& type, ::avro::NodePtr* node);
  Status Visit(const SchemaField& field, ::avro::NodePtr* node);

 private:
  // Store recently accessed field ids on the current visitor path.
  std::stack<int32_t> field_ids_;
};

}  // namespace iceberg::avro
