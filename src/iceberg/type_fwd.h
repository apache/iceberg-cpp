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

/// \brief Data type family.
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
class PrimitiveType;
class StringType;
class StructType;
class TimeType;
class TimestampBase;
class TimestampType;
class TimestampTzType;
class Type;
class UuidType;

/// \brief Data values.
class Decimal;
class Uuid;

/// \brief Schema.
class Schema;
class SchemaField;

/// \brief Partition spec and values.
class PartitionField;
class PartitionSpec;
class PartitionValues;

/// \brief Sort order.
class SortField;
class SortOrder;

/// \brief Name mapping.
struct MappedField;
class MappedFields;
class NameMapping;

/// \brief Transform.
enum class TransformType;
class Transform;
class TransformFunction;

/// \brief Table identifier.
struct Namespace;
struct TableIdentifier;

/// \brief Table metadata.
enum class SnapshotRefType;
struct MetadataLogEntry;
struct PartitionStatisticsFile;
struct Snapshot;
struct SnapshotLogEntry;
struct SnapshotRef;
struct StatisticsFile;
struct TableMetadata;
class InheritableMetadata;
class SnapshotSummaryBuilder;

/// \brief Expression.
class BoundPredicate;
class BoundReference;
class BoundTransform;
class Expression;
class Literal;
class Term;
class UnboundPredicate;

/// \brief Evaluator.
class Evaluator;
class InclusiveMetricsEvaluator;
class ManifestEvaluator;
class ResidualEvaluator;
class StrictMetricsEvaluator;

/// \brief Scan.
class DataTableScan;
class FileScanTask;
class ScanTask;
class TableScan;
class TableScanBuilder;

/// \brief Manifest.
enum class ManifestContent;
struct DataFile;
struct ManifestEntry;
struct ManifestFile;
struct ManifestList;
struct PartitionFieldSummary;
class ManifestGroup;
class ManifestListReader;
class ManifestListWriter;
class ManifestReader;
class ManifestWriter;
class PartitionSummary;

/// \brief File I/O.
struct ReaderOptions;
struct WriterOptions;
class FileIO;
class Reader;
class Writer;

/// \brief Row-based data structures.
class ArrayLike;
class MapLike;
class StructLike;
class StructLikeAccessor;

/// \brief Catalog
class Catalog;
class LocationProvider;

/// \brief Table.
class Table;
class TableProperties;

/// \brief Table update.
class TableMetadataBuilder;
class TableUpdate;
class TableRequirement;
class TableUpdateContext;
class Transaction;

/// \brief Update family.
class ExpireSnapshots;
class FastAppend;
class PendingUpdate;
class SetSnapshot;
class SnapshotUpdate;
class UpdateLocation;
class UpdatePartitionSpec;
class UpdateProperties;
class UpdateSchema;
class UpdateSortOrder;

/// ----------------------------------------------------------------------------
/// TODO: Forward declarations below are not added yet.
/// ----------------------------------------------------------------------------

class EncryptedKey;

}  // namespace iceberg
