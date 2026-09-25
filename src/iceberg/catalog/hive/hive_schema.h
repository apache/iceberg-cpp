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

#include <string>
#include <vector>

#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/hive/hive_schema.h
/// \brief Convert an Iceberg `Schema` (or any nested `Type`) into the
///        Hive DDL representation that HMS expects on FieldSchema.type.
///
/// The conversion is the C++ port of iceberg-rust's `HiveSchemaBuilder`
/// (`crates/catalog/hms/src/schema.rs`) and mirrors Java's
/// `HiveSchemaUtil` for the subset of types Hive can represent.

namespace iceberg::hive {

/// \brief A column entry for Hive's HMS table schema.
///
/// HiveCatalog converts each top-level Iceberg `SchemaField` into one of
/// these, then HmsClient further wraps them into Thrift `FieldSchema`
/// records before passing to `create_table` / `alter_table`. Nested
/// types are flattened into the `type_string` field via DDL syntax
/// (e.g. `"struct<a:int,b:string>"`, `"array<int>"`, `"map<string,int>"`).
struct ICEBERG_HIVE_EXPORT HiveColumn {
  std::string name;
  std::string type_string;
  std::string comment;
};

/// \brief Render an Iceberg `Type` as a Hive DDL type string.
///
/// Supported primitive mappings (matching iceberg-rust):
///   boolean -> boolean
///   int     -> int
///   long    -> bigint
///   float   -> float
///   double  -> double
///   date                 -> date
///   timestamp            -> timestamp
///   timestamptz          -> timestamp with local time zone
///   time / string / uuid -> string
///   binary / fixed       -> binary
///   decimal(p, s)        -> decimal(p,s)
///   struct<...>          -> struct<f:t,...>
///   list<T>              -> array<T>
///   map<K, V>            -> map<K,V>
///
/// `timestamptz` follows iceberg-java, which emits Hive's
/// `timestamp with local time zone` (TIMESTAMPLOCALTZ). Engines that
/// read the table through HMS -- Hive, Spark, Trino -- take their
/// column types from this DDL, so downgrading to a naive `timestamp`
/// would silently misrepresent a tz-adjusted column to every one of
/// them. iceberg-rust instead rejects zoned timestamps outright, which
/// would make such tables uncreatable; that behaviour is not copied.
///
/// Java picks the spelling at runtime and falls back to plain
/// `timestamp` below Hive 3, where TIMESTAMPLOCALTZ does not exist.
/// That fallback is not reproduced: iceberg-cpp has no Hive-version
/// probe, the vendored Metastore IDL is Hive 4.0.1, and Hive 2 has been
/// end-of-life since 2019.
ICEBERG_HIVE_EXPORT Result<std::string> TypeToHiveString(const Type& type);

/// \brief Convert each top-level field in `schema` to a `HiveColumn`.
///
/// `optional` is intentionally not surfaced — HMS treats every column
/// as nullable. Iceberg `doc` is copied into `comment` so DESCRIBE
/// statements in Hive show the column documentation.
ICEBERG_HIVE_EXPORT Result<std::vector<HiveColumn>> SchemaToHiveColumns(
    const Schema& schema);

}  // namespace iceberg::hive
