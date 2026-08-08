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

/// \file iceberg/geospatial.h
/// \brief Geospatial bounds and intersection utilities.

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A point used as a lower or upper geospatial column bound.
class ICEBERG_EXPORT GeospatialBound {
 public:
  /// \brief Create a two-dimensional bound.
  static GeospatialBound XY(double x, double y);
  /// \brief Create a bound with a Z coordinate.
  static GeospatialBound XYZ(double x, double y, double z);
  /// \brief Create a bound with an M coordinate.
  static GeospatialBound XYM(double x, double y, double m);
  /// \brief Create a bound with Z and M coordinates.
  static GeospatialBound XYZM(double x, double y, double z, double m);

  /// \brief Return the X coordinate.
  double x() const { return x_; }
  /// \brief Return the Y coordinate.
  double y() const { return y_; }
  /// \brief Return the optional Z coordinate.
  const std::optional<double>& z() const { return z_; }
  /// \brief Return the optional M coordinate.
  const std::optional<double>& m() const { return m_; }

  /// \brief Decode a little-endian geospatial bound.
  static Result<GeospatialBound> Deserialize(std::span<const uint8_t> bytes);
  /// \brief Encode this bound using little-endian encoding.
  std::vector<uint8_t> Serialize() const;

  friend bool operator==(const GeospatialBound&, const GeospatialBound&) = default;

 private:
  GeospatialBound(double x, double y, std::optional<double> z, std::optional<double> m);

  double x_;
  double y_;
  std::optional<double> z_;
  std::optional<double> m_;
};

/// \brief A pair of lower and upper geospatial column bounds.
class ICEBERG_EXPORT BoundingBox {
 public:
  /// \brief Create a bounding box from lower and upper bounds.
  BoundingBox(GeospatialBound lower, GeospatialBound upper);

  /// \brief Return the lower bound.
  const GeospatialBound& lower() const { return lower_; }
  /// \brief Return the upper bound.
  const GeospatialBound& upper() const { return upper_; }

  /// \brief Decode a bounding box from separate lower and upper encodings.
  static Result<BoundingBox> Deserialize(std::span<const uint8_t> lower,
                                         std::span<const uint8_t> upper);
  /// \brief Decode a bounding box from Iceberg's combined encoding.
  static Result<BoundingBox> Deserialize(std::span<const uint8_t> bytes);
  /// \brief Encode this bounding box using Iceberg's combined encoding.
  std::vector<uint8_t> Serialize() const;

  friend bool operator==(const BoundingBox&, const BoundingBox&) = default;

 private:
  GeospatialBound lower_;
  GeospatialBound upper_;
};

/// \brief Check whether two bounding boxes intersect for an Iceberg geospatial type.
///
/// Returns an error for invalid boxes or a non-geospatial type.
ICEBERG_EXPORT Result<bool> GeospatialBoundsIntersect(const Type& type,
                                                      const BoundingBox& lhs,
                                                      const BoundingBox& rhs);

}  // namespace iceberg
