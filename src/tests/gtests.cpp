#include "../blob.h"

#include <gtest/gtest.h>

#include <limits>

namespace DB36_NS
{

std::unique_ptr<Byte[]> ConvertUintKeyToByteArray(const uint64_t& key, const unsigned int& keyLength)
{
    auto retVal = std::make_unique<Byte[]>(keyLength);
    const uint64_t zero = 0;
    // fill array with zeros
    memcpy(retVal.get(), &zero, keyLength);
    if (sizeof(uint64_t) > keyLength)
    {
        memcpy(retVal.get(), &key, keyLength);
        return retVal;
    }
    else
    {
        // we'll lose some data here TBH
        memcpy(retVal.get(), &key, sizeof(uint64_t));
        return retVal;

    }
}

TEST(BlobTest, SlotOfTest)
{
    Blob b("/tmp/testblobs/blob.bl", 4, 3, 10);
    b.Init();
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(0, 4).get()), 0);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(4194304, 4).get()), 1);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(8388608, 4).get()), 2);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(16777216, 4).get()), 4);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(33554432, 4).get()), 8);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(67108864, 4).get()), 16);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(134217728, 4).get()), 32);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(268435456, 4).get()), 64);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(536870912, 4).get()), 128);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(1073741824, 4).get()), 256);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(2147483648, 4).get()), 512);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(2151677952, 4).get()), 513);
    EXPECT_EQ(b.SlotOf(ConvertUintKeyToByteArray(std::numeric_limits<uint32_t>::max(), 4).get()), 1023);
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

TEST(BlobTest, IOTest)
{
    Blob b("/tmp/testblobs/blob.bl", 3, 3, 0);
    EXPECT_NO_THROW(b.Init());
    std::unique_ptr<Byte[]>setValueBytes(new Byte[3] {254, 0, 254});
    const auto byteKey = ConvertUintKeyToByteArray(0, 3).get();
    b.Set(byteKey, setValueBytes.get(), 3);
    auto getValueBytes = b.Get(byteKey);
    for (int i = 0; i < 3; ++i)
    {
        EXPECT_EQ(setValueBytes[i], getValueBytes[i]);
    }

    const auto byteKey2 = ConvertUintKeyToByteArray(10, 3).get();
    b.Set(byteKey2, setValueBytes.get(), 3);
    getValueBytes = b.Get(byteKey2);
    for (int i = 0; i < 3; ++i)
    {
        EXPECT_EQ(setValueBytes[i], getValueBytes[i]);
    }
}

}

int main()
{
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}