#include <gtest/gtest.h>

#include "../blob.h"

// class BlobTest : public ::testing::Test
// {
//     public:
//         BlobTest() = default;
//     protected:
//         uint64_t SlotOf(const DB36_NS::BigInt& key)
//         {
//             return b.SlotOf(key);
//         }
//     private:
//         DB36_NS::Blob b;
// };

namespace DB36_NS
{

TEST(BlobTest, SlotOfTest)
{
    Blob b("/tmp/testblobs/blob.bl", 4, 3, 10);
    b.Init();
    EXPECT_EQ(b.SlotOf(0), 0);
    EXPECT_EQ(b.SlotOf(4194304), 1);
    EXPECT_EQ(b.SlotOf(8388608), 2);
    EXPECT_EQ(b.SlotOf(16777216), 4);
    EXPECT_EQ(b.SlotOf(33554432), 8);
    EXPECT_EQ(b.SlotOf(67108864), 16);
    EXPECT_EQ(b.SlotOf(134217728), 32);
    EXPECT_EQ(b.SlotOf(268435456), 64);
    EXPECT_EQ(b.SlotOf(536870912), 128);
    EXPECT_EQ(b.SlotOf(1073741824), 256);
    EXPECT_EQ(b.SlotOf(2147483648), 512);
    EXPECT_EQ(b.SlotOf(2151677952), 513);
    EXPECT_EQ(b.SlotOf(4294967296), 1024);
    EXPECT_NO_THROW(b.Close());
    EXPECT_NO_THROW(b.Destroy());
}

TEST(BlobTest, AutoCapacityTest)
{
    Blob b("/tmp/testblobs/blob.bl", 1, 2, 0);
    EXPECT_NO_THROW(b.Init());
    EXPECT_EQ(b.blobCapacity, 0);
    EXPECT_EQ(b.CapacitySize(), 512);
    EXPECT_EQ(b.RecordsCount(), 256);
    EXPECT_NO_THROW(b.Close());
    EXPECT_NO_THROW(b.Destroy());
}

}

int main()
{
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}