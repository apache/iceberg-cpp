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

#include <string_view>

namespace iceberg::avro {

// Avro logical type constants
constexpr std::string_view kMapLogicalType = "map";

// Name mapping field constants
constexpr std::string_view kElement = "element";
constexpr std::string_view kKey = "key";
constexpr std::string_view kValue = "value";

// Avro custom attributes constants
constexpr std::string_view kIcebergFieldNameProp = "iceberg-field-name";
constexpr std::string_view kFieldIdProp = "field-id";
constexpr std::string_view kKeyIdProp = "key-id";
constexpr std::string_view kValueIdProp = "value-id";
constexpr std::string_view kElementIdProp = "element-id";
constexpr std::string_view kAdjustToUtcProp = "adjust-to-utc";

}  // namespace iceberg::avro
