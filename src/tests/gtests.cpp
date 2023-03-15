#include "../blob.h"

#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <random>

namespace DB36_NS
{

std::unique_ptr<Byte[]> ConvertUintKeyToByteArray(const uint64_t& key, const unsigned int& keyLength)
{
    auto retVal = std::make_unique<Byte[]>(keyLength);
    const uint64_t zero = 0;
    // fill array with zeros
    std::memcpy(retVal.get(), &zero, keyLength);
    if (sizeof(uint64_t) > keyLength)
    {
        std::memcpy(retVal.get(), &key, keyLength);
        return retVal;
    }
    else
    {
        // we'll lose some data here TBH
        std::memcpy(retVal.get(), &key, sizeof(uint64_t));
        return retVal;
    }
}

void IOTest(const Byte* key, const Byte* data, const uint64_t& dataLen, Blob& b)
{
    const auto keyLen = b.KeyLength();
    b.Set(key, data, dataLen);
    const auto getValue = b.Get(key);
    for (int i = 0; i < dataLen; ++i)
        EXPECT_EQ(data[i], getValue[i]);
}

TEST(BlobTest, SlotOfTest)
{
    Blob b("/tmp/testblobs/blob.bl", 4, 3, 10);
    b.Init();
    const auto recLen = b.blobRecordLength;
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(0, 4).get()), 0 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(4194304, 4).get()), 1 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(8388608, 4).get()), 2 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(16777216, 4).get()), 4 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(33554432, 4).get()), 8 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(67108864, 4).get()), 16 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(134217728, 4).get()), 32 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(268435456, 4).get()), 64 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(536870912, 4).get()), 128 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(1073741824, 4).get()), 256 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(2147483648, 4).get()), 512 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(2151677952, 4).get()), 513 * recLen);
    EXPECT_EQ(b.GetKeyAddress(ConvertUintKeyToByteArray(std::numeric_limits<uint32_t>::max(), 4).get()), 1023 * recLen);
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
    IOTest(byteKey, setValueBytes.get(), 3, b);
    const auto byteKey2 = ConvertUintKeyToByteArray(10, 3).get();
    IOTest(byteKey2, setValueBytes.get(), 3, b);
    
    b.Close();
    b.Destroy();
}

TEST(BlobTest, MillionRecords)
{
    Blob b("/tmp/testblobs/blob.bl", 4, 4, 20);
    EXPECT_NO_THROW(b.Init());

    std::random_device dev;
    std::uniform_int_distribution<uint32_t> dist (0, std::numeric_limits<uint32_t>::max());
    for (int i = 0; i < 1000000; ++i)
    {
        const auto key = ConvertUintKeyToByteArray(dist(dev), 4);
        IOTest(key.get(), key.get(), 4, b);
    }

}

}

int main()
{
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}