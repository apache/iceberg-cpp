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
#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/NodeImpl.hh>
#include <avro/Schema.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>

#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_util_internal.h"
#include "iceberg/util/formatter.h"
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

::avro::LogicalType GetMapLogicalType() {
  RegisterLogicalTypes();
  return ::avro::LogicalType(std::make_shared<MapLogicalType>());
}

::avro::CustomAttributes GetAttributesWithFieldId(int32_t field_id) {
  ::avro::CustomAttributes attributes;
  attributes.addAttribute(std::string(kFieldIdProp), std::to_string(field_id),
                          /*addQuotes=*/false);
  return attributes;
}

}  // namespace

std::string ToString(const ::avro::NodePtr& node) {
  std::stringstream ss;
  ss << *node;
  return ss.str();
}

std::string ToString(const ::avro::LogicalType& logical_type) {
  std::stringstream ss;
  logical_type.printJson(ss);
  return ss.str();
}

std::string ToString(const ::avro::LogicalType::Type& logical_type) {
  return ToString(::avro::LogicalType(logical_type));
}

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
  *node = std::make_shared<::avro::NodeFixed>();
  (*node)->setName(::avro::Name("uuid_fixed"));
  (*node)->setFixedSize(16);
  (*node)->setLogicalType(::avro::LogicalType{::avro::LogicalType::UUID});
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

namespace {

bool HasId(const ::avro::NodePtr& parent_node, size_t field_idx,
           const std::string& attr_name) {
  if (field_idx >= parent_node->customAttributes()) {
    return false;
  }
  return parent_node->customAttributesAt(field_idx).getAttribute(attr_name).has_value();
}

}  // namespace

Status HasIdVisitor::Visit(const ::avro::NodePtr& node) {
  if (!node) [[unlikely]] {
    return InvalidSchema("Avro node is null");
  }

  switch (node->type()) {
    case ::avro::AVRO_RECORD:
      return VisitRecord(node);
    case ::avro::AVRO_ARRAY:
      return VisitArray(node);
    case ::avro::AVRO_MAP:
      return VisitMap(node);
    case ::avro::AVRO_UNION:
      return VisitUnion(node);
    case ::avro::AVRO_BOOL:
    case ::avro::AVRO_INT:
    case ::avro::AVRO_LONG:
    case ::avro::AVRO_FLOAT:
    case ::avro::AVRO_DOUBLE:
    case ::avro::AVRO_STRING:
    case ::avro::AVRO_BYTES:
    case ::avro::AVRO_FIXED:
      return {};
    case ::avro::AVRO_NULL:
    case ::avro::AVRO_ENUM:
    default:
      return InvalidSchema("Unsupported Avro type: {}", static_cast<int>(node->type()));
  }
}

Status HasIdVisitor::VisitRecord(const ::avro::NodePtr& node) {
  static const std::string kFieldIdKey{kFieldIdProp};
  total_fields_ += node->leaves();
  for (size_t i = 0; i < node->leaves(); ++i) {
    if (HasId(node, i, kFieldIdKey)) {
      fields_with_id_++;
    }
    ICEBERG_RETURN_UNEXPECTED(Visit(node->leafAt(i)));
  }
  return {};
}

Status HasIdVisitor::VisitArray(const ::avro::NodePtr& node) {
  if (node->leaves() != 1) [[unlikely]] {
    return InvalidSchema("Array type must have exactly one leaf");
  }

  if (node->logicalType().type() == ::avro::LogicalType::CUSTOM &&
      node->logicalType().customLogicalType() != nullptr &&
      node->logicalType().customLogicalType()->name() == "map") {
    return Visit(node->leafAt(0));
  }

  total_fields_++;
  if (HasId(node, /*field_idx=*/0, std::string(kElementIdProp))) {
    fields_with_id_++;
  }

  return Visit(node->leafAt(0));
}

Status HasIdVisitor::VisitMap(const ::avro::NodePtr& node) {
  if (node->leaves() != 2) [[unlikely]] {
    return InvalidSchema("Map type must have exactly two leaves");
  }

  total_fields_ += 2;
  if (HasId(node, /*field_idx=*/0, std::string(kKeyIdProp))) {
    fields_with_id_++;
  }
  if (HasId(node, /*field_idx=*/0, std::string(kValueIdProp))) {
    fields_with_id_++;
  }

  return Visit(node->leafAt(1));
}

Status HasIdVisitor::VisitUnion(const ::avro::NodePtr& node) {
  if (node->leaves() != 2) [[unlikely]] {
    return InvalidSchema("Union type must have exactly two branches");
  }

  const auto& branch_0 = node->leafAt(0);
  const auto& branch_1 = node->leafAt(1);
  if (branch_0->type() == ::avro::AVRO_NULL) {
    return Visit(branch_1);
  }
  if (branch_1->type() == ::avro::AVRO_NULL) {
    return Visit(branch_0);
  }

  return InvalidSchema("Union type must have exactly one null branch");
}

Status HasIdVisitor::Visit(const ::avro::ValidSchema& schema) {
  return Visit(schema.root());
}

Status HasIdVisitor::Visit(const ::avro::Schema& schema) { return Visit(schema.root()); }

namespace {

bool HasLogicalType(const ::avro::NodePtr& node,
                    ::avro::LogicalType::Type expected_type) {
  return node->logicalType().type() == expected_type;
}

std::optional<std::string> GetAdjustToUtc(const ::avro::NodePtr& node) {
  if (node->customAttributes() == 0) {
    return std::nullopt;
  }
  return node->customAttributesAt(0).getAttribute(std::string(kAdjustToUtcProp));
}

Result<int32_t> GetId(const ::avro::NodePtr& node, const std::string& attr_name,
                      size_t field_idx) {
  if (field_idx >= node->customAttributes()) {
    return InvalidSchema("Field index {} exceeds available custom attributes {}",
                         field_idx, node->customAttributes());
  }

  auto id_str = node->customAttributesAt(field_idx).getAttribute(attr_name);
  if (!id_str.has_value()) {
    return InvalidSchema("Missing avro attribute: {}", attr_name);
  }

  try {
    return std::stoi(id_str.value());
  } catch (const std::exception& e) {
    return InvalidSchema("Invalid {}: {}", attr_name, id_str.value());
  }
}

Result<int32_t> GetElementId(const ::avro::NodePtr& node) {
  static const std::string kElementIdKey{kElementIdProp};
  return GetId(node, kElementIdKey, /*field_idx=*/0);
}

Result<int32_t> GetKeyId(const ::avro::NodePtr& node) {
  static const std::string kKeyIdKey{kKeyIdProp};
  return GetId(node, kKeyIdKey, /*field_idx=*/0);
}

Result<int32_t> GetValueId(const ::avro::NodePtr& node) {
  static const std::string kValueIdKey{kValueIdProp};
  return GetId(node, kValueIdKey, /*field_idx=*/0);
}

Result<int32_t> GetFieldId(const ::avro::NodePtr& node, size_t field_idx) {
  static const std::string kFieldIdKey{kFieldIdProp};
  return GetId(node, kFieldIdKey, field_idx);
}

Status ValidateAvroSchemaEvolution(const Type& expected_type,
                                   const ::avro::NodePtr& avro_node) {
  switch (expected_type.type_id()) {
    case TypeId::kBoolean:
      if (avro_node->type() == ::avro::AVRO_BOOL) {
        return {};
      }
      break;
    case TypeId::kInt:
      if (avro_node->type() == ::avro::AVRO_INT) {
        return {};
      }
      break;
    case TypeId::kLong:
      if (avro_node->type() == ::avro::AVRO_LONG ||
          avro_node->type() == ::avro::AVRO_INT) {
        return {};
      }
      break;
    case TypeId::kFloat:
      if (avro_node->type() == ::avro::AVRO_FLOAT) {
        return {};
      }
      break;
    case TypeId::kDouble:
      if (avro_node->type() == ::avro::AVRO_DOUBLE ||
          avro_node->type() == ::avro::AVRO_FLOAT) {
        return {};
      }
      break;
    case TypeId::kDate:
      if (avro_node->type() == ::avro::AVRO_INT &&
          HasLogicalType(avro_node, ::avro::LogicalType::DATE)) {
        return {};
      }
      break;
    case TypeId::kTime:
      if (avro_node->type() == ::avro::AVRO_LONG &&
          HasLogicalType(avro_node, ::avro::LogicalType::TIME_MICROS)) {
        return {};
      }
      break;
    case TypeId::kTimestamp:
      if (avro_node->type() == ::avro::AVRO_LONG &&
          HasLogicalType(avro_node, ::avro::LogicalType::TIMESTAMP_MICROS) &&
          GetAdjustToUtc(avro_node).value_or("false") == "false") {
        return {};
      }
      break;
    case TypeId::kTimestampTz:
      if (avro_node->type() == ::avro::AVRO_LONG &&
          HasLogicalType(avro_node, ::avro::LogicalType::TIMESTAMP_MICROS) &&
          GetAdjustToUtc(avro_node).value_or("false") == "true") {
        return {};
      }
      break;
    case TypeId::kString:
      if (avro_node->type() == ::avro::AVRO_STRING) {
        return {};
      }
      break;
    case TypeId::kDecimal:
      if (avro_node->type() == ::avro::AVRO_FIXED &&
          HasLogicalType(avro_node, ::avro::LogicalType::DECIMAL)) {
        const auto& decimal_type =
            internal::checked_cast<const DecimalType&>(expected_type);
        const auto logical_type = avro_node->logicalType();
        if (decimal_type.scale() == logical_type.scale() &&
            decimal_type.precision() >= logical_type.precision()) {
          return {};
        }
      }
      break;
    case TypeId::kUuid:
      if (avro_node->type() == ::avro::AVRO_FIXED && avro_node->fixedSize() == 16 &&
          HasLogicalType(avro_node, ::avro::LogicalType::UUID)) {
        return {};
      }
      break;
    case TypeId::kFixed:
      if (avro_node->type() == ::avro::AVRO_FIXED &&
          avro_node->fixedSize() ==
              internal::checked_cast<const FixedType&>(expected_type).length()) {
        return {};
      }
      break;
    case TypeId::kBinary:
      if (avro_node->type() == ::avro::AVRO_BYTES) {
        return {};
      }
      break;
    default:
      break;
  }

  return InvalidSchema("Cannot read Iceberg type: {} from Avro type: {}", expected_type,
                       ToString(avro_node));
}

// XXX: Result<::avro::NodePtr> leads to unresolved external symbol error on Windows.
Status UnwrapUnion(const ::avro::NodePtr& node, ::avro::NodePtr* result) {
  if (node->type() != ::avro::AVRO_UNION) {
    *result = node;
    return {};
  }
  if (node->leaves() != 2) {
    return InvalidSchema("Union type must have exactly two branches");
  }
  auto branch_0 = node->leafAt(0);
  auto branch_1 = node->leafAt(1);
  if (branch_0->type() == ::avro::AVRO_NULL) {
    *result = branch_1;
  } else if (branch_1->type() == ::avro::AVRO_NULL) {
    *result = branch_0;
  } else {
    return InvalidSchema("Union type must have exactly one null branch, got {}",
                         ToString(node));
  }
  return {};
}

// Forward declaration
Result<FieldProjection> ProjectNested(const Type& expected_type,
                                      const ::avro::NodePtr& avro_node,
                                      bool prune_source);

Result<FieldProjection> ProjectStruct(const StructType& struct_type,
                                      const ::avro::NodePtr& avro_node,
                                      bool prune_source) {
  if (avro_node->type() != ::avro::AVRO_RECORD) {
    return InvalidSchema("Expected AVRO_RECORD type, but got {}", ToString(avro_node));
  }

  const auto& expected_fields = struct_type.fields();

  struct NodeInfo {
    size_t local_index;
    ::avro::NodePtr field_node;
  };
  std::unordered_map<int32_t, NodeInfo> node_info_map;
  node_info_map.reserve(avro_node->leaves());

  for (size_t i = 0; i < avro_node->leaves(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(int32_t field_id, GetFieldId(avro_node, i));
    ::avro::NodePtr field_node = avro_node->leafAt(i);
    if (const auto [iter, inserted] = node_info_map.emplace(
            std::piecewise_construct, std::forward_as_tuple(field_id),
            std::forward_as_tuple(i, field_node));
        !inserted) [[unlikely]] {
      return InvalidSchema("Duplicate field id found in Avro schema: {}", field_id);
    }
  }

  FieldProjection result;
  result.children.reserve(expected_fields.size());

  for (const auto& expected_field : expected_fields) {
    int32_t field_id = expected_field.field_id();
    FieldProjection child_projection;

    if (auto iter = node_info_map.find(field_id); iter != node_info_map.cend()) {
      ::avro::NodePtr field_node;
      ICEBERG_RETURN_UNEXPECTED(UnwrapUnion(iter->second.field_node, &field_node));
      if (expected_field.type()->is_nested()) {
        ICEBERG_ASSIGN_OR_RAISE(
            child_projection,
            ProjectNested(*expected_field.type(), field_node, prune_source));
      } else {
        ICEBERG_RETURN_UNEXPECTED(
            ValidateAvroSchemaEvolution(*expected_field.type(), field_node));
      }
      child_projection.from = iter->second.local_index;
      child_projection.kind = FieldProjection::Kind::kProjected;
    } else if (MetadataColumns::IsMetadataColumn(field_id)) {
      child_projection.kind = FieldProjection::Kind::kMetadata;
    } else if (expected_field.optional()) {
      child_projection.kind = FieldProjection::Kind::kNull;
    } else {
      return InvalidSchema("Missing required field with ID: {}", field_id);
    }

    result.children.emplace_back(std::move(child_projection));
  }

  if (prune_source) {
    PruneFieldProjection(result);
  }

  return result;
}

Result<FieldProjection> ProjectList(const ListType& list_type,
                                    const ::avro::NodePtr& avro_node, bool prune_source) {
  if (avro_node->type() != ::avro::AVRO_ARRAY) {
    return InvalidSchema("Expected AVRO_ARRAY type, but got {}", ToString(avro_node));
  }
  if (avro_node->leaves() != 1) {
    return InvalidSchema("Array type must have exactly one node, got {}",
                         avro_node->leaves());
  }

  const auto& expected_element_field = list_type.fields().back();
  ICEBERG_ASSIGN_OR_RAISE(int32_t avro_element_id, GetElementId(avro_node));
  if (expected_element_field.field_id() != avro_element_id) [[unlikely]] {
    return InvalidSchema("element-id mismatch, expected {}, got {}",
                         expected_element_field.field_id(), avro_element_id);
  }

  FieldProjection element_projection;
  ::avro::NodePtr element_node;
  ICEBERG_RETURN_UNEXPECTED(UnwrapUnion(avro_node->leafAt(0), &element_node));
  if (expected_element_field.type()->is_nested()) {
    ICEBERG_ASSIGN_OR_RAISE(
        element_projection,
        ProjectNested(*expected_element_field.type(), element_node, prune_source));
  } else {
    ICEBERG_RETURN_UNEXPECTED(
        ValidateAvroSchemaEvolution(*expected_element_field.type(), element_node));
  }

  // Set the element projection metadata but preserve its children
  element_projection.kind = FieldProjection::Kind::kProjected;
  element_projection.from = size_t{0};

  FieldProjection result;
  result.children.emplace_back(std::move(element_projection));
  return result;
}

Result<FieldProjection> ProjectMap(const MapType& map_type,
                                   const ::avro::NodePtr& avro_node, bool prune_source) {
  const auto& expected_key_field = map_type.key();
  const auto& expected_value_field = map_type.value();

  FieldProjection result, key_projection, value_projection;
  int32_t avro_key_id, avro_value_id;
  ::avro::NodePtr map_node;

  if (avro_node->type() == ::avro::AVRO_MAP) {
    if (avro_node->leaves() != 2) {
      return InvalidSchema("Map type must have exactly two nodes, got {}",
                           avro_node->leaves());
    }
    map_node = avro_node;

    ICEBERG_ASSIGN_OR_RAISE(avro_key_id, GetKeyId(avro_node));
    ICEBERG_ASSIGN_OR_RAISE(avro_value_id, GetValueId(avro_node));
  } else if (avro_node->type() == ::avro::AVRO_ARRAY && HasMapLogicalType(avro_node)) {
    if (avro_node->leaves() != 1) {
      return InvalidSchema("Array-backed map type must have exactly one node, got {}",
                           avro_node->leaves());
    }

    map_node = avro_node->leafAt(0);
    if (map_node->type() != ::avro::AVRO_RECORD || map_node->leaves() != 2) {
      return InvalidSchema(
          "Array-backed map type must have a record node with two fields");
    }

    ICEBERG_ASSIGN_OR_RAISE(avro_key_id, GetFieldId(map_node, 0));
    ICEBERG_ASSIGN_OR_RAISE(avro_value_id, GetFieldId(map_node, 1));
  } else {
    return InvalidSchema("Expected a map type, but got Avro type {}",
                         ToString(avro_node));
  }

  if (expected_key_field.field_id() != avro_key_id) {
    return InvalidSchema("key-id mismatch, expected {}, got {}",
                         expected_key_field.field_id(), avro_key_id);
  }
  if (expected_value_field.field_id() != avro_value_id) {
    return InvalidSchema("value-id mismatch, expected {}, got {}",
                         expected_value_field.field_id(), avro_value_id);
  }

  for (size_t i = 0; i < map_node->leaves(); ++i) {
    FieldProjection sub_projection;
    ::avro::NodePtr sub_node;
    ICEBERG_RETURN_UNEXPECTED(UnwrapUnion(map_node->leafAt(i), &sub_node));
    const auto& expected_sub_field = map_type.fields()[i];
    if (expected_sub_field.type()->is_nested()) {
      ICEBERG_ASSIGN_OR_RAISE(sub_projection, ProjectNested(*expected_sub_field.type(),
                                                            sub_node, prune_source));
    } else {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateAvroSchemaEvolution(*expected_sub_field.type(), sub_node));
    }
    sub_projection.kind = FieldProjection::Kind::kProjected;
    sub_projection.from = i;
    result.children.emplace_back(std::move(sub_projection));
  }

  return result;
}

Result<FieldProjection> ProjectNested(const Type& expected_type,
                                      const ::avro::NodePtr& avro_node,
                                      bool prune_source) {
  if (!expected_type.is_nested()) {
    return InvalidSchema("Expected a nested type, but got {}", expected_type);
  }

  switch (expected_type.type_id()) {
    case TypeId::kStruct:
      return ProjectStruct(internal::checked_cast<const StructType&>(expected_type),
                           avro_node, prune_source);
    case TypeId::kList:
      return ProjectList(internal::checked_cast<const ListType&>(expected_type),
                         avro_node, prune_source);
    case TypeId::kMap:
      return ProjectMap(internal::checked_cast<const MapType&>(expected_type), avro_node,
                        prune_source);
    default:
      return InvalidSchema("Unsupported nested type: {}", expected_type);
  }
}

}  // namespace

bool HasMapLogicalType(const ::avro::NodePtr& node) {
  return node->logicalType().type() == ::avro::LogicalType::CUSTOM &&
         node->logicalType().customLogicalType() != nullptr &&
         node->logicalType().customLogicalType()->name() == "map";
}

Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const ::avro::NodePtr& avro_node, bool prune_source) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto field_projection,
      ProjectNested(static_cast<const Type&>(expected_schema), avro_node, prune_source));
  return SchemaProjection{std::move(field_projection.children)};
}

}  // namespace iceberg::avro
