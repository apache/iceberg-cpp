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

#include "iceberg/geospatial.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "iceberg/type.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr size_t kDoubleSize = sizeof(double);

std::optional<double> OptionalOrdinate(double value) {
  return std::isnan(value) ? std::nullopt : std::make_optional(value);
}

template <EndianConvertible T>
void AppendLittleEndian(std::vector<uint8_t>& bytes, T value) {
  const size_t offset = bytes.size();
  bytes.resize(offset + sizeof(value));
  WriteLittleEndian(value, bytes.data() + offset);
}

bool IsValidBoundSize(size_t size) {
  return size == 2 * kDoubleSize || size == 3 * kDoubleSize || size == 4 * kDoubleSize;
}

bool RangeIntersects(double min1, double max1, double min2, double max2) {
  return min1 <= max2 && max1 >= min2;
}

bool RangeIntersectsWithWrapAround(double min1, double max1, double min2, double max2) {
  // A wrapped longitude interval [min, max], where min > max, represents
  // [min, 180] plus [-180, max].
  const bool interval1_wraps = min1 > max1;
  const bool interval2_wraps = min2 > max2;
  if (!interval1_wraps && !interval2_wraps) {
    return RangeIntersects(min1, max1, min2, max2);
  }
  if (interval1_wraps && interval2_wraps) {
    // Both intervals contain the antimeridian, so they always intersect.
    return true;
  }
  if (interval1_wraps) {
    return min1 <= max2 || max1 >= min2;
  }
  return min2 <= max1 || max2 >= min1;
}

Status ValidateOptionalRanges(const BoundingBox& bbox) {
  if (bbox.lower().z().has_value() && bbox.upper().z().has_value() &&
      *bbox.lower().z() > *bbox.upper().z()) {
    return InvalidArgument("Invalid Z range: zmin cannot be greater than zmax");
  }
  if (bbox.lower().m().has_value() && bbox.upper().m().has_value() &&
      *bbox.lower().m() > *bbox.upper().m()) {
    return InvalidArgument("Invalid M range: mmin cannot be greater than mmax");
  }
  return {};
}

Status ValidateGeometry(const BoundingBox& bbox) {
  if (!(bbox.lower().x() <= bbox.upper().x())) {
    return InvalidArgument("Invalid X range: xmin cannot be greater than xmax");
  }
  if (!(bbox.lower().y() <= bbox.upper().y())) {
    return InvalidArgument("Invalid Y range: ymin cannot be greater than ymax");
  }
  return ValidateOptionalRanges(bbox);
}

Status ValidateGeography(const BoundingBox& bbox) {
  if (!(bbox.lower().y() >= -90.0 && bbox.lower().y() <= 90.0 &&
        bbox.upper().y() >= -90.0 && bbox.upper().y() <= 90.0)) {
    return InvalidArgument("Invalid latitude: out of range [-90, 90]");
  }
  if (!(bbox.lower().x() >= -180.0 && bbox.lower().x() <= 180.0 &&
        bbox.upper().x() >= -180.0 && bbox.upper().x() <= 180.0)) {
    return InvalidArgument("Invalid longitude: out of range [-180, 180]");
  }
  if (bbox.lower().y() > bbox.upper().y()) {
    return InvalidArgument("Invalid latitude range: ymin cannot be greater than ymax");
  }
  return ValidateOptionalRanges(bbox);
}

bool IntersectsYzm(const BoundingBox& lhs, const BoundingBox& rhs) {
  if (lhs.lower().z().has_value() && lhs.upper().z().has_value() &&
      rhs.lower().z().has_value() && rhs.upper().z().has_value() &&
      !RangeIntersects(*lhs.lower().z(), *lhs.upper().z(), *rhs.lower().z(),
                       *rhs.upper().z())) {
    return false;
  }
  if (lhs.lower().m().has_value() && lhs.upper().m().has_value() &&
      rhs.lower().m().has_value() && rhs.upper().m().has_value() &&
      !RangeIntersects(*lhs.lower().m(), *lhs.upper().m(), *rhs.lower().m(),
                       *rhs.upper().m())) {
    return false;
  }
  return RangeIntersects(lhs.lower().y(), lhs.upper().y(), rhs.lower().y(),
                         rhs.upper().y());
}

}  // namespace

GeospatialBound::GeospatialBound(double x, double y, std::optional<double> z,
                                 std::optional<double> m)
    : x_(x), y_(y), z_(std::move(z)), m_(std::move(m)) {}

GeospatialBound GeospatialBound::XY(double x, double y) {
  return GeospatialBound{x, y, std::nullopt, std::nullopt};
}

GeospatialBound GeospatialBound::XYZ(double x, double y, double z) {
  return GeospatialBound{x, y, OptionalOrdinate(z), std::nullopt};
}

GeospatialBound GeospatialBound::XYM(double x, double y, double m) {
  return GeospatialBound{x, y, std::nullopt, OptionalOrdinate(m)};
}

GeospatialBound GeospatialBound::XYZM(double x, double y, double z, double m) {
  return GeospatialBound{x, y, OptionalOrdinate(z), OptionalOrdinate(m)};
}

Result<GeospatialBound> GeospatialBound::Deserialize(std::span<const uint8_t> bytes) {
  if (!IsValidBoundSize(bytes.size())) {
    return InvalidArgument(
        "Invalid geospatial bound size: {}. Valid sizes are 16, 24, or 32 bytes",
        bytes.size());
  }

  auto x = ReadLittleEndian<double>(bytes.data());
  auto y = ReadLittleEndian<double>(bytes.data() + kDoubleSize);
  if (bytes.size() == 2 * kDoubleSize) {
    return XY(x, y);
  }

  auto z = ReadLittleEndian<double>(bytes.data() + 2 * kDoubleSize);
  if (bytes.size() == 3 * kDoubleSize) {
    return XYZ(x, y, z);
  }
  auto m = ReadLittleEndian<double>(bytes.data() + 3 * kDoubleSize);
  if (std::isnan(z)) {
    return XYM(x, y, m);
  }
  return XYZM(x, y, z, m);
}

std::vector<uint8_t> GeospatialBound::Serialize() const {
  size_t size = 2 * kDoubleSize;
  if (z_.has_value() && !m_.has_value()) {
    size = 3 * kDoubleSize;
  } else if (m_.has_value()) {
    size = 4 * kDoubleSize;
  }

  std::vector<uint8_t> bytes;
  bytes.reserve(size);
  AppendLittleEndian(bytes, x_);
  AppendLittleEndian(bytes, y_);
  if (z_.has_value() || m_.has_value()) {
    AppendLittleEndian(bytes, z_.value_or(std::numeric_limits<double>::quiet_NaN()));
  }
  if (m_.has_value()) {
    AppendLittleEndian(bytes, *m_);
  }
  return bytes;
}

BoundingBox::BoundingBox(GeospatialBound lower, GeospatialBound upper)
    : lower_(std::move(lower)), upper_(std::move(upper)) {}

Result<BoundingBox> BoundingBox::Deserialize(std::span<const uint8_t> lower,
                                             std::span<const uint8_t> upper) {
  ICEBERG_ASSIGN_OR_RAISE(auto lower_bound, GeospatialBound::Deserialize(lower));
  ICEBERG_ASSIGN_OR_RAISE(auto upper_bound, GeospatialBound::Deserialize(upper));
  return BoundingBox(std::move(lower_bound), std::move(upper_bound));
}

Result<BoundingBox> BoundingBox::Deserialize(std::span<const uint8_t> bytes) {
  constexpr size_t kLengthSize = sizeof(uint32_t);
  if (bytes.size() < kLengthSize) {
    return InvalidArgument("Truncated bounding box encoding");
  }
  const size_t lower_size = ReadLittleEndian<uint32_t>(bytes.data());
  if (!IsValidBoundSize(lower_size) ||
      bytes.size() < kLengthSize + lower_size + kLengthSize) {
    return InvalidArgument("Invalid or truncated lower geospatial bound");
  }

  const size_t upper_length_offset = kLengthSize + lower_size;
  const size_t upper_size =
      ReadLittleEndian<uint32_t>(bytes.data() + upper_length_offset);
  const size_t upper_offset = upper_length_offset + kLengthSize;
  if (!IsValidBoundSize(upper_size) || bytes.size() < upper_offset + upper_size) {
    return InvalidArgument("Invalid or truncated upper geospatial bound");
  }

  return Deserialize(bytes.subspan(kLengthSize, lower_size),
                     bytes.subspan(upper_offset, upper_size));
}

std::vector<uint8_t> BoundingBox::Serialize() const {
  auto lower_bytes = lower_.Serialize();
  auto upper_bytes = upper_.Serialize();
  std::vector<uint8_t> bytes;
  bytes.reserve(sizeof(uint32_t) + lower_bytes.size() + sizeof(uint32_t) +
                upper_bytes.size());
  AppendLittleEndian(bytes, static_cast<uint32_t>(lower_bytes.size()));
  bytes.insert(bytes.end(), lower_bytes.begin(), lower_bytes.end());
  AppendLittleEndian(bytes, static_cast<uint32_t>(upper_bytes.size()));
  bytes.insert(bytes.end(), upper_bytes.begin(), upper_bytes.end());
  return bytes;
}

Result<bool> GeospatialBoundsIntersect(const Type& type, const BoundingBox& lhs,
                                       const BoundingBox& rhs) {
  switch (type.type_id()) {
    case TypeId::kGeometry:
      ICEBERG_RETURN_UNEXPECTED(ValidateGeometry(lhs));
      ICEBERG_RETURN_UNEXPECTED(ValidateGeometry(rhs));
      if (!IntersectsYzm(lhs, rhs)) {
        return false;
      }
      return RangeIntersects(lhs.lower().x(), lhs.upper().x(), rhs.lower().x(),
                             rhs.upper().x());
    case TypeId::kGeography:
      ICEBERG_RETURN_UNEXPECTED(ValidateGeography(lhs));
      ICEBERG_RETURN_UNEXPECTED(ValidateGeography(rhs));
      if (!IntersectsYzm(lhs, rhs)) {
        return false;
      }
      return RangeIntersectsWithWrapAround(lhs.lower().x(), lhs.upper().x(),
                                           rhs.lower().x(), rhs.upper().x());
    default:
      return NotSupported("Unsupported type for BoundingBox: {}", type.ToString());
  }
}

}  // namespace iceberg
