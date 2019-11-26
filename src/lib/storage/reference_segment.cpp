#include <storage/reference_segment.hpp>

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos):
  _referenced_table{referenced_table},
  _referenced_column_id{referenced_column_id},
  _pos{pos} {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const auto row_id = _pos->at(chunk_offset);
  return referenced_table()->get_chunk(row_id.chunk_id).get_segment(_referenced_column_id)->operator[](chunk_offset);
}

size_t ReferenceSegment::size() const { return _pos->size(); }

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos; }

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceSegment::estimate_memory_usage() const {
  return sizeof(RowID) * _pos->capacity();
}

void ReferenceSegment::segment_scan(const AllTypeVariant &value,  &result) const {
  uint32_t chunk_count = _referenced_table->chunk_count();
  std::vector<std::vector<uint32_t>> indices_filter_for_chunk(chunk_count);

  for (const auto &item: *_pos) {
    indices_filter_for_chunk[item.chunk_id].push_back(item.chunk_offset);
  }

  for (uint32_t chunk_index = 0; chunk_index < chunk_count; chunk_index++) {
    const auto &segment = referenced_table()->get_chunk(ChunkID(chunk_index)).get_segment(_referenced_column_id);
    segment->segment_scan()
  }
}


std::shared_ptr<PosList> ReferenceSegment::get_indexes_of_value(const AllTypeVariant &value,
                                                                const std::shared_ptr<uint32_t> &indices_filter,
                                                                const uint32_t chunk_id,
                                                                PosList &result) const {

}

}
