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

#include <format>
#include <mutex>
#include <string_view>

#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>

#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

namespace iceberg::avro {

namespace {

constexpr std::string_view kIcebergFieldNameProp = "iceberg-field-name";
constexpr std::string_view kFieldIdProp = "field-id";
constexpr std::string_view kKeyIdProp = "key-id";
constexpr std::string_view kValueIdProp = "value-id";
constexpr std::string_view kElementIdProp = "element-id";
constexpr std::string_view kAdjustToUtcProp = "adjust-to-utc";

struct MapLogicalType : public ::avro::CustomLogicalType {
  MapLogicalType() : ::avro::CustomLogicalType("map") {}
};

::avro::LogicalType GetMapLogicalType() {
  static std::once_flag flag{};
  std::call_once(flag, []() {
    // Register the map logical type with the avro custom logical type registry.
    // See https://github.com/apache/avro/pull/3326 for details.
    ::avro::CustomLogicalTypeRegistry::instance().registerType(
        "map", [](const std::string&) { return std::make_shared<MapLogicalType>(); });
  });
  return ::avro::LogicalType(std::make_shared<MapLogicalType>());
}

::avro::CustomAttributes GetAttributesWithFieldId(int32_t field_id) {
  ::avro::CustomAttributes attributes;
  attributes.addAttribute(std::string(kFieldIdProp), std::to_string(field_id),
                          /*addQuotes=*/false);
  return attributes;
}

}  // namespace

Status ToAvroNodeVisitor::Visit(const BooleanType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_BOOL);
  return {};
}

Status ToAvroNodeVisitor::Visit(const IntType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_INT);
  return {};
}

Status ToAvroNodeVisitor::Visit(const LongType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_LONG);
  return {};
}

Status ToAvroNodeVisitor::Visit(const FloatType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_FLOAT);
  return {};
}

Status ToAvroNodeVisitor::Visit(const DoubleType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_DOUBLE);
  return {};
}

Status ToAvroNodeVisitor::Visit(const DecimalType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodeFixed>();
  (*node)->setName(
      ::avro::Name(std::format("decimal_{}_{}", type.precision(), type.scale())));
  (*node)->setFixedSize(::arrow::DecimalType::DecimalSize(type.precision()));

  ::avro::LogicalType logical_type(::avro::LogicalType::DECIMAL);
  logical_type.setPrecision(type.precision());
  logical_type.setScale(type.scale());
  (*node)->setLogicalType(logical_type);

  return {};
}

Status ToAvroNodeVisitor::Visit(const DateType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_INT);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::DATE});
  return {};
}

Status ToAvroNodeVisitor::Visit(const TimeType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_LONG);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::TIME_MICROS});
  return {};
}

Status ToAvroNodeVisitor::Visit(const TimestampType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_LONG);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::TIMESTAMP_MICROS});
  ::avro::CustomAttributes attributes;
  attributes.addAttribute(std::string(kAdjustToUtcProp), "false", /*addQuotes=*/false);
  (*node)->addCustomAttributesForField(attributes);
  return {};
}

Status ToAvroNodeVisitor::Visit(const TimestampTzType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_LONG);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::TIMESTAMP_MICROS});
  ::avro::CustomAttributes attributes;
  attributes.addAttribute(std::string(kAdjustToUtcProp), "true", /*addQuotes=*/false);
  (*node)->addCustomAttributesForField(attributes);
  return {};
}

Status ToAvroNodeVisitor::Visit(const StringType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_STRING);
  return {};
}

Status ToAvroNodeVisitor::Visit(const UuidType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_FIXED);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::UUID});
  (*node)->setFixedSize(16);
  (*node)->setName(::avro::Name("uuid_fixed"));
  return {};
}

Status ToAvroNodeVisitor::Visit(const FixedType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodeFixed>();
  (*node)->setName(::avro::Name(std::format("fixed_{}", type.length())));
  (*node)->setFixedSize(type.length());
  return {};
}

Status ToAvroNodeVisitor::Visit(const BinaryType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodePrimitive>(::avro::AVRO_BYTES);
  return {};
}

Status ToAvroNodeVisitor::Visit(const StructType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodeRecord>();

  if (field_ids_.empty()) {
    (*node)->setName(::avro::Name("iceberg_schema"));  // Root node
  } else {
    (*node)->setName(::avro::Name(std::format("r{}", field_ids_.top())));
  }

  for (const SchemaField& sub_field : type.fields()) {
    ::avro::NodePtr field_node;
    ICEBERG_RETURN_UNEXPECTED(Visit(sub_field, &field_node));

    // TODO(gangwu): sanitize field name
    (*node)->addName(std::string(sub_field.name()));
    (*node)->addLeaf(field_node);
    (*node)->addCustomAttributesForField(GetAttributesWithFieldId(sub_field.field_id()));
  }
  return {};
}

Status ToAvroNodeVisitor::Visit(const ListType& type, ::avro::NodePtr* node) {
  *node = std::make_shared<::avro::NodeArray>();
  const auto& element_field = type.fields().back();

  ::avro::CustomAttributes attributes;
  attributes.addAttribute(std::string(kElementIdProp),
                          std::to_string(element_field.field_id()),
                          /*addQuotes=*/false);

  ::avro::NodePtr element_node;
  ICEBERG_RETURN_UNEXPECTED(Visit(element_field, &element_node));

  (*node)->addCustomAttributesForField(attributes);
  (*node)->addLeaf(std::move(element_node));
  return {};
}

Status ToAvroNodeVisitor::Visit(const MapType& type, ::avro::NodePtr* node) {
  const auto& key_field = type.key();
  const auto& value_field = type.value();

  if (key_field.optional()) [[unlikely]] {
    return InvalidArgument("Map key `{}` must be required", key_field.name());
  }

  if (key_field.type()->type_id() == TypeId::kString) {
    ::avro::CustomAttributes attributes;
    attributes.addAttribute(std::string(kKeyIdProp), std::to_string(key_field.field_id()),
                            /*addQuotes=*/false);
    attributes.addAttribute(std::string(kValueIdProp),
                            std::to_string(value_field.field_id()),
                            /*addQuotes=*/false);

    ::avro::NodePtr value_node;
    ICEBERG_RETURN_UNEXPECTED(Visit(value_field, &value_node));

    *node = std::make_shared<::avro::NodeMap>();
    (*node)->addLeaf(std::move(value_node));
    (*node)->addCustomAttributesForField(attributes);
  } else {
    auto struct_node = std::make_shared<::avro::NodeRecord>();
    struct_node->setName(::avro::Name(
        std::format("k{}_v{}", key_field.field_id(), value_field.field_id())));

    ::avro::NodePtr key_node;
    ICEBERG_RETURN_UNEXPECTED(Visit(key_field, &key_node));
    struct_node->addLeaf(std::move(key_node));
    struct_node->addName("key");
    struct_node->addCustomAttributesForField(
        GetAttributesWithFieldId(key_field.field_id()));

    ::avro::NodePtr value_node;
    ICEBERG_RETURN_UNEXPECTED(Visit(value_field, &value_node));
    struct_node->addLeaf(std::move(value_node));
    struct_node->addName("value");
    struct_node->addCustomAttributesForField(
        GetAttributesWithFieldId(value_field.field_id()));

    *node = std::make_shared<::avro::NodeArray>();
    (*node)->addLeaf(std::move(struct_node));
    (*node)->setLogicalType(GetMapLogicalType());
  }

  return {};
}

Status ToAvroNodeVisitor::Visit(const SchemaField& field, ::avro::NodePtr* node) {
  field_ids_.push(field.field_id());
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*field.type(), /*visitor=*/this, node));

  if (field.optional()) {
    ::avro::MultiLeaves union_types;
    union_types.add(std::make_shared<::avro::NodePrimitive>(::avro::AVRO_NULL));
    union_types.add(std::move(*node));
    *node = std::make_shared<::avro::NodeUnion>(union_types);
  }

  field_ids_.pop();
  return {};
}

}  // namespace iceberg::avro
