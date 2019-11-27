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

void ReferenceSegment::segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID) const {
  uint32_t chunk_count = _referenced_table->chunk_count();
  std::vector<std::vector<ChunkOffset>> offset_filter_for_chunk(chunk_count);

  for (const auto &item: *_pos) {
    offset_filter_for_chunk[item.chunk_id].push_back(item.chunk_offset);
  }

  for (uint32_t chunk_index = 0; chunk_index < chunk_count; chunk_index++) {
    const auto &segment = referenced_table()->get_chunk(ChunkID(chunk_index)).get_segment(_referenced_column_id);
    segment->segment_scan(compare_value, scan_op, result_callback, ChunkID(chunk_index), offset_filter_for_chunk[chunk_index]);
  }
}


void ReferenceSegment::segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID, std::vector<ChunkOffset> offset_filter) const {
  uint32_t chunk_count = _referenced_table->chunk_count();
  std::vector<std::vector<ChunkOffset>> offset_filter_for_chunk(chunk_count);

  for (const auto offset: offset_filter) {
    const auto &item = (*_pos)[offset];
    offset_filter_for_chunk[item.chunk_id].push_back(item.chunk_offset);
  }

  for (uint32_t chunk_index = 0; chunk_index < chunk_count; chunk_index++) {
    const auto &segment = referenced_table()->get_chunk(ChunkID(chunk_index)).get_segment(_referenced_column_id);
    segment->segment_scan(compare_value, scan_op, result_callback, ChunkID(chunk_index), offset_filter_for_chunk[chunk_index]);
  }
}

}
