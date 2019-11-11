#pragma once

#include <vector>
#include "storage/base_attribute_vector.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(std::vector<T> value_ids) : _value_ids{value_ids} {}

  // returns the value id at a given position
  virtual ValueID get(const size_t index) const {
    DebugAssert(index < size(), "Index out of bounds");
    return ValueID(_value_ids[index]);
  }

  // sets the value id at a given position
  virtual void set(const size_t i, const ValueID value_id) {
    Assert(i < _value_ids.size(), "Out of range");
    _value_ids[i] = value_id;
  }

  // returns the number of values
  virtual size_t size() const { return _value_ids.size(); }

  // returns the width of biggest value id in bytes
  virtual AttributeVectorWidth width() const { return sizeof(T); }

  // returns the calculated memory usage
  virtual size_t estimate_memory_usage() const { return _value_ids.capacity() * sizeof(T); }

 protected:
  std::vector<T> _value_ids;
};
}  // namespace opossum
