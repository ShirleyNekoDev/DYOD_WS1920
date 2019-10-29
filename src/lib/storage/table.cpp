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

Table::Table(const u_int32_t chunk_size) : _max_chunk_size{chunk_size} {
  // create initial chunk to make things easier
  _append_new_chunk();
}

void Table::_append_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  for (auto& type : _column_types) {
    // append existing columns (as segments) to new chunk
    new_chunk->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
  }
  _chunks.push_back(new_chunk);
}

void Table::_append_column_to_chunks(const std::string& type) {
  for (auto& chunk : _chunks) {
    // append new segment to every existing chunk
    chunk->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
  }
}

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
  _append_column_to_chunks(type);
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back()->size() >= _max_chunk_size) {
    // create a new chunk if the current one is full
    _append_new_chunk();
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const { return _column_types.size(); }

uint64_t Table::row_count() const {
  auto full_chunks_row_count = (_chunks.size() - 1) * _max_chunk_size;
  auto last_chunk_row_count = _chunks.back()->size();
  return full_chunks_row_count + last_chunk_row_count;
}

ChunkID Table::chunk_count() const { return ChunkID(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto search_index_iterator = std::find(_column_names.begin(), _column_names.end(), column_name);
  if (search_index_iterator == _column_names.end()) {
    throw std::invalid_argument("Column not found");
  }
  return ColumnID(std::distance(_column_names.begin(), search_index_iterator));
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "Column id out of bounds");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "Column id out of bounds");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < column_count(), "Chunk id out of bounds");
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return get_chunk(chunk_id); }

}  // namespace opossum
