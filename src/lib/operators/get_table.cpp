#include <storage/storage_manager.hpp>
#include <operators/get_table.hpp>

namespace opossum {

GetTable::GetTable(const std::string& name):
  _table_name{name} {}

const std::string& GetTable::table_name() const {
  return _table_name;
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  return StorageManager::get().get_table(_table_name);
}

}
