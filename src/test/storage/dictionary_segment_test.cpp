#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");

  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);
}

TEST_F(StorageDictionarySegmentTest, CompressSegmentMedium) {
  for (int i = 0; i < 256; i++) {
    vc_int->append(i);
  }

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  vc_int->append(42);
  vc_int->append(70000);
  auto col2 = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col2 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col2);

  EXPECT_EQ(dict_col->unique_values_count(), 256u);
  EXPECT_EQ(dict_col2->unique_values_count(), 257u);
  EXPECT_EQ(dict_col->size(), 256u);
  EXPECT_EQ(dict_col2->size(), 258u);

  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(dict_col->dictionary()->at(i), i);
    EXPECT_EQ(dict_col2->dictionary()->at(i), i);
  }

  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);
  EXPECT_EQ(dict_col2->attribute_vector()->width(), 2);
}

TEST_F(StorageDictionarySegmentTest, CompressSegmentLarge) {
  for (int i = 0; i < 65536; i++) {
    vc_int->append(i);
  }

  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  vc_int->append(42);
  vc_int->append(70000);
  auto col2 = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col2 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col2);

  EXPECT_EQ(dict_col->unique_values_count(), 65536u);
  EXPECT_EQ(dict_col2->unique_values_count(), 65537u);
  EXPECT_EQ(dict_col->size(), 65536u);
  EXPECT_EQ(dict_col2->size(), 65538u);

  for (int i = 0; i < 65536; i++) {
    EXPECT_EQ(dict_col->dictionary()->at(i), i);
    EXPECT_EQ(dict_col2->dictionary()->at(i), i);
  }

  EXPECT_EQ(dict_col->attribute_vector()->width(), 2);
  EXPECT_EQ(dict_col2->attribute_vector()->width(), 4);
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = opossum::make_shared_by_data_type<opossum::BaseSegment, opossum::DictionarySegment>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.
