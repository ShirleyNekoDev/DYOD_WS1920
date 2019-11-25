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

virtual PosList get_indeces_of_value(const AllTypeVariant &value) const {

}


}
