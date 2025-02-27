#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 private:
  const std::shared_ptr<const AbstractOperator> _in;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan() override;

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
};

}  // namespace opossum
