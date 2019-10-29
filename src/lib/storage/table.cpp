#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const u_int32_t chunk_size) : _max_chunk_size(chunk_size) {
  // create initial chunk to make things easier
  _append_new_chunk();
}

void Table::_append_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  _chunks.push_back(new_chunk);
}

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
  // add value segment
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Implementation goes here
}

uint16_t Table::column_count() const {
  return _column_types.size();
}

uint64_t Table::row_count() const {
  // Implementation goes here
  return 0;
}

ChunkID Table::chunk_count() const {
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto search_index_iterator = std::find(_column_names.begin(), _column_names.end(), column_name);
  if(search_index_iterator == _column_names.end()) {
    throw std::invalid_argument("Column not found");
  }
  return ColumnID(std::distance(_column_names.begin(), search_index_iterator));
}

uint32_t Table::max_chunk_size() const {
  return _max_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(ColumnID column_id) const {
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return get_chunk(chunk_id);
}

}  // namespace opossum
