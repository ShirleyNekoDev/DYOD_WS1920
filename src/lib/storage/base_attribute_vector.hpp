#pragma once

#include "types.hpp"

namespace opossum {

// BaseAttributeVector is the abstract super class for all attribute vectors,
// e.g., FixedSizeAttributeVector
class BaseAttributeVector : private Noncopyable {
 public:
  BaseAttributeVector() = default;
  virtual ~BaseAttributeVector() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseAttributeVector(BaseAttributeVector&&) = default;
  BaseAttributeVector& operator=(BaseAttributeVector&&) = default;

  // returns the value id at a given position
  virtual ValueID get(const size_t i) const = 0;

  // sets the value id at a given position
  virtual void set(const size_t i, const ValueID value_id) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // returns the width of biggest value id in bytes
  virtual AttributeVectorWidth width() const = 0;

  // returns the calculated memory usage
  virtual size_t estimate_memory_usage() const = 0;
};
}  // namespace opossum
