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

  for (uint16_t column_id = 0; column_id < result->column_count(); column_id++) {
    result->add_column(input->column_name(ColumnID(column_id)), input->column_type(ColumnID(column_id)));
  }

  //Assert(input->column_type(_column_id) == detail::type_strings[_search_value.type()]);

  for (uint32_t chunk_index = 0; chunk_index < input->chunk_count(); chunk_index++) {
    auto segment = input->get_chunk(ChunkID(chunk_index)).get_segment(_column_id);
    auto pos_list = segment->get_indeces_of_value(_search_value);
    auto result_chunk = std::make_shared<Chunk>();

    for (uint16_t column_id = 0; column_id < input->column_count(); column_id++) {
      result_chunk->add_segment(std::make_shared<ReferenceSegment>(input, ColumnID(column_id), pos_list));
    }

    //auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment>(segment);
  }


  return _output;
}

}
