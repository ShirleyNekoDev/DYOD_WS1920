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
  auto input = _in->get_output();
  auto result = std::make_shared<Table>();

  const uint16_t column_count = input->column_count();
  const uint32_t chunk_size = input->max_chunk_size();

  for (uint16_t column_id = 0; column_id < column_count; column_id++) {
    result->add_column_definition(input->column_name(ColumnID(column_id)), input->column_type(ColumnID(column_id)));
  }

  //Assert(input->column_type(_column_id) == detail::type_strings[_search_value.type()]);

  std::vector<RowID> row_ids;

  for (uint32_t input_chunk_index = 0; input_chunk_index < input->chunk_count(); input_chunk_index++) {
    auto segment = input->get_chunk(ChunkID(input_chunk_index)).get_segment(_column_id);
    segment->segment_scan(_search_value, _scan_type, [&column_count, &input, &input_chunk_index, &result, &row_ids, &chunk_size](ChunkOffset chunk_offset) {
      if (row_ids.size() == chunk_size) {
        Chunk new_chunk;
        for (uint16_t column_id = 0; column_id < column_count; column_id++) {
          new_chunk.add_segment(std::make_shared<ReferenceSegment>(input, ColumnID(column_id), row_ids));
        }
        result->emplace_chunk(std::move(new_chunk));
      }

      RowID new_row_id;
      row_ids.push_back(RowID{ChunkID(input_chunk_index), chunk_offset});
    });
  }


  return _output;
}

}
