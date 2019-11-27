#pragma once

#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/base_segment.hpp"
#include "storage/fixed_size_attribute_vector.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseAttributeVector;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

const auto ALWAYS_TRUE_SCAN_PREDICATE = [](const ValueID& value_id) { return true; };
const auto ALWAYS_FALSE_SCAN_PREDICATE = [](const ValueID& value_id) { return false; };

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(base_segment);
    Assert(value_segment, "Input should be a ValueSegment of same data type");

    const auto& column_values = value_segment->values();
    Assert(column_values.size() > 0, "Segment has no elements");

    _compress_values(column_values);
  }

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override { return get(chunk_offset); }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const { return _dictionary->at(_attribute_vector->get(chunk_offset)); }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override { throw std::logic_error("dictionary segments are immutable"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto iterator = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    if (iterator == _dictionary->end()) {
      return INVALID_VALUE_ID;
    } else {
      return ValueID(std::distance(_dictionary->begin(), iterator));
    }
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto iterator = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    if (iterator == _dictionary->end()) {
      return INVALID_VALUE_ID;
    } else {
      return ValueID(std::distance(_dictionary->begin(), iterator));
    }
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // returns the position of the search value in the segment if it is found
  // returns INVALID_VALUE_ID otherwise
  ValueID find_value(T value) const {
    const ValueID probable_value_id = lower_bound(value);
    if(probable_value_id != INVALID_VALUE_ID) {
      const T& found_value = this->value_by_value_id(probable_value_id);
      if(found_value == value) {
       return probable_value_id;
      }
    }
    return INVALID_VALUE_ID;
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final {
    const size_t dictionary_size = _dictionary->capacity() * sizeof(T);
    return dictionary_size + _attribute_vector->estimate_memory_usage();
  }
  
  // scans every value in this segment and calls the result_callback if the scan_op comparison with compare_value returns true
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id) const override {
    const auto row_count = _attribute_vector->size();
    const auto scan_predicate = _scan_predicate(type_cast<T>(compare_value), scan_op);
    for(ChunkOffset row_index = 0; row_index < row_count; ++row_index) {
      if(scan_predicate(_attribute_vector->get(row_index))) {
        result_callback(RowID{chunk_id, row_index});
      }
    }
  }

  // same as above, but only using the values at offsets from offset_filter
  virtual void segment_scan(const AllTypeVariant& compare_value, const ScanType scan_op, const std::function<void(RowID)> result_callback, ChunkID chunk_id, std::vector<ChunkOffset> row_filter) const override {
    const auto scan_predicate = _scan_predicate(type_cast<T>(compare_value), scan_op);
    for(const ChunkOffset row_index: row_filter) {
      if(scan_predicate(_attribute_vector->get(row_index))) {
        result_callback(RowID{chunk_id, row_index});
      }
    }
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

  const std::function<bool(const ValueID&)> _scan_predicate(const T& compare_value, const ScanType scan_op) const {
    // optimized scan - we can use the order of _dictionary and it's indices to filter the elements via binary search
    ValueID found_value_id, lower_bounds_id, upper_bounds_id;
    switch(scan_op) {
      case ScanType::OpEquals:
        found_value_id = find_value(compare_value);
        if(found_value_id != INVALID_VALUE_ID) {
          // value found, filter rows by matching id
          return [&found_value_id](const ValueID& value_id) { return value_id == found_value_id; };
        } else {
          // value not found, no row will match
          return ALWAYS_FALSE_SCAN_PREDICATE;
        }
      case ScanType::OpNotEquals:
        found_value_id = find_value(compare_value);
        if(found_value_id != INVALID_VALUE_ID) {
          // value found, filter rows by non-matching id
          return [&found_value_id](const ValueID& value_id) { return value_id != found_value_id; };
        } else {
          // value not found, all rows will match
          return ALWAYS_TRUE_SCAN_PREDICATE;
        }
      case ScanType::OpLessThan:
        lower_bounds_id = lower_bound(compare_value);
        if (lower_bounds_id != INVALID_VALUE_ID) {
          // some values are smaller, filter rows with smaller id
          return [&lower_bounds_id](const ValueID& value_id) { return value_id < lower_bounds_id; };
        } else {
          // all values are smaller
          return ALWAYS_TRUE_SCAN_PREDICATE;
        }
      case ScanType::OpLessThanEquals:
        upper_bounds_id = upper_bound(compare_value);
        if (upper_bounds_id != INVALID_VALUE_ID) {
          // some values are smaller or equal, filter rows with smaller id
          return [&upper_bounds_id](const ValueID& value_id) { return value_id < upper_bounds_id; };
        } else {
          // all values are smaller
          return ALWAYS_TRUE_SCAN_PREDICATE;
        }
      case ScanType::OpGreaterThan:
        upper_bounds_id = upper_bound(compare_value);
        if (upper_bounds_id != INVALID_VALUE_ID) {
          // some values are bigger, filter rows with bigger or equal id
          return [&upper_bounds_id](const ValueID& value_id) { return value_id >= upper_bounds_id; };
        } else {
          // all values are bigger
          return ALWAYS_FALSE_SCAN_PREDICATE;
        }
      case ScanType::OpGreaterThanEquals:
        lower_bounds_id = lower_bound(compare_value);
        if (lower_bounds_id != INVALID_VALUE_ID) {
          // some values are bigger or equal, filter rows with bigger or equal id
          return [&lower_bounds_id](const ValueID& value_id) { return value_id >= lower_bounds_id; };
        } else {
          // all values are bigger
          return ALWAYS_FALSE_SCAN_PREDICATE;
        }
      default:
        throw std::domain_error("Unknown scan operation");
    }
  }

  void _compress_values(const std::vector<T>& column_values) {
    std::vector<uint32_t> lookup_indices(column_values.size());
    // initialized with many falses
    std::vector<bool> same_as_previous_element(column_values.size());

    std::iota(lookup_indices.begin(), lookup_indices.end(), 0);
    std::sort(lookup_indices.begin(), lookup_indices.end(),
              [&column_values](const uint32_t index_1, const uint32_t index_2) {
                return column_values[index_1] < column_values[index_2];
              });

    uint32_t previous_uncompressed_index = lookup_indices[0];
    uint32_t num_unique = 1;
    for (uint32_t position = 1; position < column_values.size(); ++position) {
      uint32_t uncompressed_index = lookup_indices[position];
      if (column_values[previous_uncompressed_index] == column_values[uncompressed_index]) {
        same_as_previous_element[position] = true;
      } else {
        ++num_unique;
      }
      previous_uncompressed_index = uncompressed_index;
    }

    if (num_unique <= static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()) + 1) {
      _build_dictionary_and_attributes<uint8_t>(column_values, lookup_indices, same_as_previous_element, num_unique);
    } else if (num_unique <= static_cast<uint32_t>(std::numeric_limits<uint16_t>::max()) + 1) {
      _build_dictionary_and_attributes<uint16_t>(column_values, lookup_indices, same_as_previous_element, num_unique);
    } else {
      _build_dictionary_and_attributes<uint32_t>(column_values, lookup_indices, same_as_previous_element, num_unique);
    }
  }

  template <typename IndexType>
  void _build_dictionary_and_attributes(const std::vector<T>& values, const std::vector<uint32_t>& indices,
                                        const std::vector<bool>& same_as_before, const uint32_t num_unique) {
    std::vector<IndexType> attributes(values.size());
    _dictionary = std::make_shared<std::vector<T>>();
    _dictionary->reserve(num_unique);

    for (uint32_t position = 0; position < values.size(); position++) {
      uint32_t uncompressed_index = indices[position];
      if (!same_as_before[position]) {
        _dictionary->push_back(values[uncompressed_index]);
      }
      attributes[uncompressed_index] = _dictionary->size() - 1;
    }

    _attribute_vector = std::static_pointer_cast<BaseAttributeVector>(
        std::make_shared<FixedSizeAttributeVector<IndexType>>(std::move(attributes)));
  }
};

}  // namespace opossum
