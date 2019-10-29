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

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  _columns.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  // TODO: error handling on index out of bounds case?
  // Implementation goes here
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  // TODO: error handling on index out of bounds case?
  return _columns[column_id];
}

uint16_t Chunk::column_count() const {
  return _columns.size();
}

uint32_t Chunk::size() const {
  if(_columns.empty()) {
    // no columns -> empty
    return 0;
  } else {
    // read row count from first column (each should have the same height)
    return _columns[0].get()->size();
  }
}

}  // namespace opossum
