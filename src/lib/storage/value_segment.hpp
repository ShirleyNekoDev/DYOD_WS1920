#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"

namespace opossum {

// ValueSegment is a segment type that stores all its values in a vector
template <typename T>
class ValueSegment : public BaseSegment {
 public:
  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  // add a value to the end
  void append(const AllTypeVariant& val) final;

  // return the number of entries
  size_t size() const final;

  // Return all values. This is the preferred method to check a value at a certain index. Usually you need to
  // access more than a single value anyway.
  // e.g. const auto& values = value_segment.values(); and then: values[i]; in your loop.
  const std::vector<T>& values() const;

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final;
  
  // scans every value in this segment and calls the result_callback if the scan_op comparison with compare_value returns true
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id) const override;

  // same as above, but only using the values at offsets from offset_filter
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id, std::vector<ChunkOffset> offset_filter) const override;

 protected:
  std::vector<T> _values;

  const std::function<bool(const T&)> _scan_predicate(const T& compare_value, const ScanType scan_op) const {
    switch (scan_op) {
      case ScanType::OpEquals:
        return [&compare_value](const T &row_value) { return row_value == compare_value; };
      case ScanType::OpNotEquals:
        return [&compare_value](const T &row_value) { return row_value != compare_value; };
      case ScanType::OpLessThan:
        return [&compare_value](const T &row_value) { return row_value < compare_value; };
      case ScanType::OpLessThanEquals:
        return [&compare_value](const T &row_value) { return row_value <= compare_value; };
      case ScanType::OpGreaterThan:
        return [&compare_value](const T &row_value) { return row_value > compare_value; };
      case ScanType::OpGreaterThanEquals:
        return [&compare_value](const T &row_value) { return row_value >= compare_value; };
      default:
        throw std::domain_error("Unknown scan operation");
    }
  }
};

}  // namespace opossum
