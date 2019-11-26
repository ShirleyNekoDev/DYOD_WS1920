#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// BaseSegment is the abstract super class for all segment types,
// e.g., ValueSegment, ReferenceSegment
class BaseSegment : private Noncopyable {
 public:
  BaseSegment() = default;
  virtual ~BaseSegment() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseSegment(BaseSegment&&) = default;
  BaseSegment& operator=(BaseSegment&&) = default;

  // returns the value at a given position
  virtual AllTypeVariant operator[](const ChunkOffset chunk_offset) const = 0;

  // appends the value at the end of the segment
  virtual void append(const AllTypeVariant& val) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // returns the calculated memory usage
  virtual size_t estimate_memory_usage() const = 0;

  virtual std::shared_ptr<PosList> get_indeces_of_value(const AllTypeVariant &value) const = 0;
  virtual std::shared_ptr<PosList> get_indeces_of_value(const AllTypeVariant &value,
                                                        const std::shared_ptr<PosList> &positions_filter) const = 0;
};
}  // namespace opossum
