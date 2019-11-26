#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < _values.size(), "Chunk offset out of bounds");
  return _values[chunk_offset];
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  _values.push_back(type_cast<T>(val));
}

template <typename T>
size_t ValueSegment<T>::size() const {
  return _values.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _values;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return _values.capacity() * sizeof(T);
}

template <typename T>
void ValueSegment<T>::segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(ChunkOffset)> result_callback) const {
  const auto row_count = size();
  const auto scan_predicate = _scan_predicate(type_cast<T>(compare_value), scan_op);
  for(ChunkOffset row_index = 0; row_index < row_count; ++row_index) {
    if(scan_predicate(_values[row_index])) {
      result_callback(row_index);
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
