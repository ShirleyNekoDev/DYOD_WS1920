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
virtual void ValueSegment<T>::segment_scan(const T& value, const ScanType scan_op, const std::function<void(ChunkOffset)> result_callback) const {
  auto size = size();
  auto scan_predicate = _scan_predicate(value, scan_op);
  T& current_value;
  for(ChunkOffset row_index = 0; row_index < size; ++row_index) {
    current_value = _values[row_index];
    if(scan_predicate(current_value)) {
      result_emplacer(current_value);
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
