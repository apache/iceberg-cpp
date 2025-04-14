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

/// \file iceberg/type_fwd.h
/// Forward declarations and enum definitions.  When writing your own headers,
/// you can include this instead of the "full" headers to help reduce compile
/// times.

namespace iceberg {

/// \brief A data type.
///
/// This is not a complete data type by itself because some types are nested
/// and/or parameterized.
///
/// Iceberg V3 types are not currently supported.
enum class TypeId {
  kStruct,
  kList,
  kMap,
  kBoolean,
  kInt,
  kLong,
  kFloat,
  kDouble,
  kDecimal,
  kDate,
  kTime,
  kTimestamp,
  kTimestampTz,
  kString,
  kUuid,
  kFixed,
  kBinary,
};

/// \brief The time unit.  In Iceberg V3 nanoseconds are also supported.
enum class TimeUnit {
  kMicrosecond,
};

class BinaryType;
class BooleanType;
class DateType;
class DecimalType;
class FixedType;
class FloatType;
class DoubleType;
class IntType;
class LongType;
class ListType;
class MapType;
class NestedType;
class PartitionField;
class PartitionSpec;
class PrimitiveType;
class Schema;
class SchemaField;
class StringType;
class StructType;
class TimeType;
class TimestampBase;
class TimestampType;
class TimestampTzType;
class Type;
class UuidType;

struct Namespace;
struct TableIdentifier;

class Catalog;
class LocationProvider;
class SortField;
class SortOrder;
class Table;
class Transaction;
class Transform;
class TransformFunction;

struct PartitionStatisticsFile;
struct Snapshot;
struct SnapshotRef;
struct StatisticsFile;
struct TableMetadata;

enum class SnapshotRefType;
enum class TransformType;

/// ----------------------------------------------------------------------------
/// TODO: Forward declarations below are not added yet.
/// ----------------------------------------------------------------------------

class HistoryEntry;
class StructLike;

class MetadataUpdate;
class UpdateRequirement;

class AppendFiles;
class TableScan;

}  // namespace iceberg
