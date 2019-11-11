#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/fixed_size_attribute_vector.hpp"

class FixedSizeAttibuteVectorTest : public ::testing::Test {
};

TEST_F(FixedSizeAttibuteVectorTest, SimpleMesthodsTest) {
    opossum::FixedSizeAttributeVector<uint16_t> attributes({3, 4, 34, 5, 1});

    EXPECT_EQ(attributes.size(), 5);
    EXPECT_EQ(attributes.get(3), 34);
    attributes.set(3, opossum::ValueID(43));
    EXPECT_EQ(attributes.get(3), 43);
}



// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.
