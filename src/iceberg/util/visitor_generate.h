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

namespace iceberg {

#define ICEBERG_GENERATE_FOR_ALL_TYPES(ACTION) \
  ACTION(Boolean);                             \
  ACTION(Int);                                 \
  ACTION(Long);                                \
  ACTION(Float);                               \
  ACTION(Double);                              \
  ACTION(Decimal);                             \
  ACTION(Date);                                \
  ACTION(Time);                                \
  ACTION(Timestamp);                           \
  ACTION(TimestampTz);                         \
  ACTION(String);                              \
  ACTION(Uuid);                                \
  ACTION(Fixed);                               \
  ACTION(Binary);                              \
  ACTION(Struct);                              \
  ACTION(List);                                \
  ACTION(Map);

/// \brief Generate switch-case for schema visitor pattern
///
/// This macro generates switch cases that dispatch to visitor methods based on type:
/// - Struct types -> calls ACTION with Struct
/// - List types -> calls ACTION with List
/// - Map types -> calls ACTION with Map
/// - All primitive types (default) -> calls ACTION with Primitive
#define ICEBERG_TYPE_SWITCH_WITH_PRIMITIVE_DEFAULT(ACTION) \
  case ::iceberg::TypeId::kStruct:                         \
    ACTION(Struct)                                         \
  case ::iceberg::TypeId::kList:                           \
    ACTION(List)                                           \
  case ::iceberg::TypeId::kMap:                            \
    ACTION(Map)                                            \
  default:                                                 \
    ACTION(Primitive)

}  // namespace iceberg
