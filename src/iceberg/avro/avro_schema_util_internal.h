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

#include "iceberg/name_mapping.h"
#include "iceberg/result.h"
#include "iceberg/schema_util.h"
#include "iceberg/type.h"

namespace avro {
class Schema;
class ValidSchema;
}  // namespace avro

namespace iceberg::avro {

struct MapLogicalType : public ::avro::CustomLogicalType {
  MapLogicalType() : ::avro::CustomLogicalType("map") {}
};

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

/// \brief A visitor that checks the presence of field IDs in an Avro schema.
class HasIdVisitor {
 public:
  HasIdVisitor() = default;

  /// \brief Visit an Avro node to check for field IDs.
  /// \param node The Avro node to visit.
  /// \return Status indicating success or an error if unsupported Avro types are
  /// encountered.
  Status Visit(const ::avro::NodePtr& node);

  /// \brief Visit an Avro schema to check for field IDs.
  /// \param schema The Avro schema to visit.
  /// \return Status indicating success or an error if unsupported Avro types are
  /// encountered.
  Status Visit(const ::avro::ValidSchema& schema);

  /// \brief Visit an Avro schema to check for field IDs.
  /// \param schema The Avro schema to visit.
  /// \return Status indicating success or an error if unsupported Avro types are
  /// encountered.
  Status Visit(const ::avro::Schema& node);

  /// \brief Check if all fields in the visited schema have field IDs.
  /// \return True if all fields have IDs, false otherwise.
  bool AllHaveIds() const {
    return total_fields_ == fields_with_id_ && fields_with_id_ != 0;
  }

  /// \brief Check if all fields in the visited schema have field IDs.
  /// \return True if all fields have IDs, false otherwise.
  bool HasNoIds() const { return total_fields_ == 0; }

 private:
  /// \brief Visit a record node to check for field IDs.
  /// \param node The record node to visit.
  /// \return Status indicating success or error.
  Status VisitRecord(const ::avro::NodePtr& node);

  /// \brief Visit an array node to check for element IDs.
  /// \param node The array node to visit.
  /// \return Status indicating success or error.
  Status VisitArray(const ::avro::NodePtr& node);

  /// \brief Visit a map node to check for key and value IDs.
  /// \param node The map node to visit.
  /// \return Status indicating success or error.
  Status VisitMap(const ::avro::NodePtr& node);

  /// \brief Visit a union node to check for field IDs in each branch.
  /// \param node The union node to visit.
  /// \return Status indicating success or error.
  Status VisitUnion(const ::avro::NodePtr& node);

 private:
  // Total number of fields visited.
  size_t total_fields_ = 0;
  // Number of fields with IDs.
  size_t fields_with_id_ = 0;
};

/// \brief Project an Iceberg Schema onto an Avro NodePtr.
///
/// This function creates a projection from an Iceberg Schema to an Avro schema node.
/// The projection determines how to read data from the Avro schema into the expected
/// Iceberg Schema.
///
/// \param expected_schema The Iceberg Schema that defines the expected structure.
/// \param avro_node The Avro node to read data from.
/// \param prune_source Whether the source schema can be pruned.
/// \return The schema projection result.
Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const ::avro::NodePtr& avro_node, bool prune_source);

std::string ToString(const ::avro::NodePtr& node);
std::string ToString(const ::avro::LogicalType& logical_type);
std::string ToString(const ::avro::LogicalType::Type& logical_type);

/// \brief Check if an Avro node has a map logical type.
/// \param node The Avro node to check.
/// \return True if the node has a map logical type, false otherwise.
bool HasMapLogicalType(const ::avro::NodePtr& node);

/// \brief Create a new Avro node with field IDs from name mapping.
/// \param original_node The original Avro node to copy.
/// \param mapped_field The mapped field to apply field IDs from.
/// \return A new Avro node with field IDs applied, or an error.
Result<::avro::NodePtr> MakeAvroNodeWithFieldIds(const ::avro::NodePtr& original_node,
                                                 const MappedField& mapped_field);

/// \brief Create a new Avro node with field IDs from name mapping.
/// \param original_node The original Avro node to copy.
/// \param mapping The name mapping to apply field IDs from.
/// \return A new Avro node with field IDs applied, or an error.
Result<::avro::NodePtr> MakeAvroNodeWithFieldIds(const ::avro::NodePtr& original_node,
                                                 const NameMapping& mapping);

}  // namespace iceberg::avro
