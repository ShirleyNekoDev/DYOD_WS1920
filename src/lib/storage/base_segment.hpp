#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// BaseSegment is the abstract super class for all segment types,
// e.g., ValueSegment, ReferenceSegment
class BaseSegment : private Noncopyable {
 public:
  BaseSegment() = default;
  virtual ~BaseSegment() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseSegment(BaseSegment&&) = default;
  BaseSegment& operator=(BaseSegment&&) = default;

  // returns the value at a given position
  virtual AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // appends the value at the end of the segment
  virtual void append(const AllTypeVariant& val) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // returns the calculated memory usage
  virtual size_t estimate_memory_usage() const = 0;

  // scans every value in this segment and calls the result_callback if the scan_op comparison with compare_value returns true
  // may be optimized in overridden implementations 
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id) const = 0;
  // scans only the values at offsets in offset_filter
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id, std::vector<ChunkOffset> offset_filter) const = 0;
};
}  // namespace opossum
