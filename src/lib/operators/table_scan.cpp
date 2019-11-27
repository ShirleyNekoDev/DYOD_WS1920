#include <storage/table.hpp>
#include <storage/value_segment.hpp>
#include <storage/dictionary_segment.hpp>
#include <storage/reference_segment.hpp>
#include <resolve_type.hpp>
#include <operators/table_scan.hpp>

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value):
  _in{in},
  _column_id{column_id},
  _scan_type{scan_type},
  _search_value{search_value} {}

TableScan::~TableScan() {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto input_table = _in->get_output();
  const auto table_column_count = input_table->column_count();
  const auto table_chunk_count = input_table->chunk_count();
  const auto table_max_chunk_size = input_table->max_chunk_size();

  auto result = std::make_shared<Table>();

  // copy column definitions
  for (ColumnID column_id = ColumnID{0}; column_id < table_column_count; column_id++) {
    result->add_column_definition(
      input_table->column_name(ColumnID(column_id)),
      input_table->column_type(ColumnID(column_id))
    );
  }

  // if the table we are scanning is already a reference table, we create new references into the
  // original table
  auto referenced_table = input_table;
  if (input_table->row_count() > 0) {
    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_table->get_chunk(ChunkID(0)).get_segment(ColumnID(0)));
    referenced_table = reference_segment->referenced_table();
  }

  auto pos_list = std::make_shared<PosList>();

  auto output_chunk = [&referenced_table, &table_column_count, &result](const std::shared_ptr<PosList> pos_list) {
    // copy pos_list into chunk
    Chunk new_chunk;
    for (uint16_t column_id = 0; column_id < table_column_count; column_id++) {
      // link all found positions for each column
      new_chunk.add_segment(std::make_shared<ReferenceSegment>(
        referenced_table,
        ColumnID(column_id),
        pos_list
      ));
    }
    // write chunk in result table
    result->emplace_chunk(std::move(new_chunk));
  };

  // scan all chunks from the table
  for (ChunkID chunk_index = ChunkID{0}; chunk_index < table_chunk_count; ++chunk_index) {
    // find the relevant column to scan
    const auto segment = input_table->get_chunk(chunk_index).get_segment(_column_id);

    // scan segment and get matching values' position via lambda
    segment->segment_scan(_search_value, _scan_type, [&](RowID row_id) {
      pos_list->push_back(row_id);

      if (pos_list->size() == table_max_chunk_size) {
        output_chunk(pos_list);
        // create new pos_list
        pos_list = std::make_shared<PosList>();
      }
    }, chunk_index);
    output_chunk(pos_list);
  }

  return result;
}

}
