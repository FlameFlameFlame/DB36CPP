#include "../blob.h"

#include <chrono>
#include <iostream>
#include <numeric>
#include <random>

class KeyValuePair
{
private:
    std::unique_ptr<DB36_NS::Byte[]> key;
    std::unique_ptr<DB36_NS::Byte[]> value;

    int keyLength;
    int valueLength;
public:
    KeyValuePair():
    keyLength(0), valueLength(0),
    key(std::make_unique<DB36_NS::Byte[]>(1)), value(std::make_unique<DB36_NS::Byte[]>(1))
    {};
    KeyValuePair(const int& i_keyLength, const int& i_valueLength) :
    keyLength(i_keyLength), valueLength(i_valueLength),
    key(new DB36_NS::Byte[i_keyLength]), value (new DB36_NS::Byte[i_valueLength])
    {};

    KeyValuePair(KeyValuePair&) = delete;
    KeyValuePair& operator= (KeyValuePair&) = delete;

    KeyValuePair(KeyValuePair&& moved) noexcept :
    keyLength(moved.keyLength),
    valueLength(moved.valueLength),
    key(std::move(moved.key)),
    value(std::move(moved.value))
    {}
    KeyValuePair& operator=(KeyValuePair&& moved) noexcept
    {
        if (this != &moved)
        {
            key = std::move(moved.key);
            value = std::move(moved.value);
            keyLength = moved.keyLength;
            valueLength = moved.valueLength;
        }
        return *this;
    }

    union u64bytes
    {
        uint64_t number;
        DB36_NS::Byte bytes[sizeof(uint64_t)];
    };

    void GenerateRandomKey()
    {
        std::random_device dev;
        std::uniform_int_distribution<uint64_t> dist (0, std::numeric_limits<uint64_t>::max());
        u64bytes u;
        for (int i = 0; i < keyLength; ++i)
        {
            if (i % sizeof(u64bytes) == 0)
            {
                u = { dist(dev) };
            }
            key.get()[i] = u.bytes[i % sizeof(u64bytes)];
        }
    }

    void GenerateRandomValue()
    {
        std::random_device dev;
        std::uniform_int_distribution<uint64_t> dist (0, std::numeric_limits<uint64_t>::max());
        u64bytes u;
        for (int i = 0; i < keyLength; ++i)
        {
            if (i % sizeof(u64bytes) == 0)
            {
                u = { dist(dev) };
            }
            value.get()[i] = u.bytes[i % sizeof(u64bytes)];
        }
    }

    void GenerateRandomKeyAndValue()
    {
        std::random_device dev;
        std::uniform_int_distribution<uint64_t> dist (0, std::numeric_limits<uint64_t>::max());
        u64bytes u;
        for (int i = 0; i < std::max(keyLength, valueLength); ++i)
        {
            if (i % sizeof(u64bytes) == 0)
            {
                u = { dist(dev) };               
            }
            if (i < keyLength)
                key.get()[i] = u.bytes[i % sizeof(u64bytes)];
            if (i < valueLength)
                value.get()[i] = u.bytes[i % sizeof(u64bytes)];
        }
    }

    int GetValueLength() const
    {
        return valueLength;
    }

    int GetKeyLength() const
    {
        return keyLength;
    }

    DB36_NS::Byte* GetKey() const
    {
        return key.get();
    }

    DB36_NS::Byte* GetValue() const
    {
        return value.get();
    }
};

std::vector<KeyValuePair> generateRandomKeyValuesVector(const int& vectorLength, const int& keyLength, const int& valueLength)
{
    int i = 0;
    std::vector<KeyValuePair> resultVector(vectorLength); // , std::move(KeyValuePair(keyLength, valueLength)));
    for (auto& keyValue : resultVector)
    {
        KeyValuePair tempKey(keyLength, valueLength);
        tempKey.GenerateRandomKeyAndValue();
        keyValue = std::move(tempKey);
        ++i;
        if (i >= 100000 && i % 100000 == 0)
            std::cout << "Generated " << i << " random values" << std::endl;
    }
    return resultVector;
}

bool IOTest(DB36_NS::Byte* key, DB36_NS::Byte* data, const uint64_t& dataLen, DB36_NS::Blob& b)
{
    const auto keyLen = b.KeyLength();
    b.Set(key, data, dataLen);
    const auto getValue = b.Get(key);
    for (int i = 0; i < dataLen; ++i)
    {
        if (data[i] != getValue.get()[i])
            return false;
    }
    return true;
}

int main(int argc, char *argv[])
{
    using namespace DB36_NS;
    using namespace std::chrono;
    using namespace std::chrono_literals;

    const int keyLength = std::stoi(argv[2]);
    const int valueLength = std::stoi(argv[3]);
    const int capacity = std::stoi(argv[1]);
    const int vectorLength = std::stoi(argv[4]);

    std::cout << "Generating large random vector, it might take a while" << std::endl;
    const auto largeVector = generateRandomKeyValuesVector(vectorLength, keyLength, valueLength);

    Blob b ("/tmp/testblobs/blob.bl", keyLength, valueLength, capacity);

    std::cout << "keyLength\t" << keyLength << std::endl;
    std::cout << "valueLength\t" << valueLength << std::endl;
    std::cout << "capacity\t" << capacity << std::endl;
    std::cout << "vectorLength\t" << vectorLength << std::endl;
    std::cout << "Test started" << std::endl;

    const auto testStartTime = system_clock::now();
    std::vector<unsigned long long> durationMicrosendsVector;
    for (auto kv = largeVector.begin(); kv != largeVector.end(); std::advance(kv, 1))
    {
        const auto IOStartTime = system_clock::now();
        if (!IOTest(kv->GetKey(), kv->GetValue(), kv->GetValueLength(), b))
            throw (std::logic_error("Key-value pair doesn't match with set values"));
        durationMicrosendsVector.emplace_back(duration_cast<microseconds>(system_clock::now() - IOStartTime).count());
    }
    const auto testDurationMilliseconds = duration_cast<milliseconds>(system_clock::now() - testStartTime).count();
    std::cout << "Test finished" << std::endl;
    std::cout << "duration\t" << testDurationMilliseconds << "ms\n";
    std::cout << "average IO\t" << double(std::accumulate(durationMicrosendsVector.begin(), durationMicrosendsVector.end(), 0)/durationMicrosendsVector.size())/1000 << "ms\n";
    std::cout << "maximal IO\t" << double(*std::max_element(durationMicrosendsVector.begin(), durationMicrosendsVector.end()))/1000 << "ms\n";
    std::cout << "minimal IO\t" << double(*std::min_element(durationMicrosendsVector.begin(), durationMicrosendsVector.end()))/1000 << "ms\n";

    return 0;
}