#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _instance;
  return _instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  auto insertion_result = _tables.insert({name, table});
  DebugAssert(insertion_result.second,
              "Table cound not be inserted because there was an existing table with the same name.");
}

void StorageManager::drop_table(const std::string& name) {
  auto dropped_table_count = _tables.erase(name);
  DebugAssert(dropped_table_count == 1, "Table could not be removed because it was not found with this name.");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const {
  return _tables.find(name) != _tables.end();
  // return _tables.contains(name); // with C++20
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  names.reserve(_tables.size());
  for (auto& map_entry : _tables) {
    names.push_back(map_entry.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  out << _tables.size() << " tables available:" << std::endl;
  for (auto& map_entry : _tables) {
    auto& table_name = map_entry.first;
    auto& table = map_entry.second;
    out << " - \"" << table_name << "\" ["
        << "column_count=" << table->column_count() << ","
        << " row_count=" << table->row_count() << ","
        << " chunk_count=" << table->chunk_count() << "]" << std::endl;
  }
}

void StorageManager::reset() {
  // clear content of storage manager (all registered tables)
  _tables.clear();
}

}  // namespace opossum
