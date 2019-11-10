#include <iostream>

#include "../lib/utils/assert.hpp"

#include "../lib/all_type_variant.hpp"
#include "../lib/storage/value_segment.hpp"

int main() {
  //opossum::Assert(true, "We can use opossum files here :)");

    /*
  opossum::AllTypeVariant var;
  var = 2.0;
  std::cout << "Variant Type: " << var.which() << std::endl;
  */

    opossum::ValueSegment<int32_t> mySegment;
    mySegment.append(42);

    int32_t value = boost::get<int32_t>(mySegment[0]);

    std::cout << "value: " << value << std::endl;

  return 0;
}
