#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _columns.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Row size mismatch while appending a new row.");
  ColumnID max_bounds_index = (ColumnID)column_count();
  for (ColumnID current_index = (ColumnID)0; current_index < max_bounds_index; ++current_index) {
    _columns[current_index].get()->append(values[current_index]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  // TODO: error handling on index out of bounds case?
  return _columns[column_id];
}

uint16_t Chunk::column_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) {
    // no columns -> empty
    return 0;
  } else {
    // read row count from first column (each should have the same height)
    return _columns[0].get()->size();
  }
}

}  // namespace opossum
