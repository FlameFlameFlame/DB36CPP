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

bool CompareByteArrays(DB36_NS::Byte* value1, DB36_NS::Byte* value2, const int& valueLength)
{
    for (int i = 0; i < valueLength; ++i)
        if (value1[i] != value2[i])
            return false;
    
    return true;
}

std::vector<KeyValuePair> GenerateRandomKeyValuesVector(const int& vectorLength, const int& keyLength, const int& valueLength)
{
    int i = 0;
    std::vector<KeyValuePair> resultVector(vectorLength);
    using KeyType = std::unique_ptr<DB36_NS::Byte[]>;

    std::vector<KeyType> usedKeys;
    for (auto& keyValue : resultVector)
    {
        KeyValuePair tempKey(keyLength, valueLength);
        tempKey.GenerateRandomValue();
        tempKey.GenerateRandomKey();
        
        keyValue = std::move(tempKey);
        ++i;
        if (i >= 100000 && i % 100000 == 0)
            std::cout << "Generated " << i << " random key and value" << std::endl;
    }
    return resultVector;
}


void FindAndReplaceAllNonUniqueKeysInVector(std::vector<KeyValuePair>& v)
{
    if (v.empty())
        throw std::logic_error("Empty vector");

    std::cout << "Looking for duplicate keys" << std::endl;
    using KeyType = std::unique_ptr<DB36_NS::Byte[]>;
    std::vector<KeyType> alreadyUsedKeys;
    
    bool iteratedThroughAllVector = false;
    const auto keyLength = v[0].GetKeyLength();
    int iteration = 0;
    while (!iteratedThroughAllVector)
    {
        std::cout << "Starting iteration " << iteration << std::endl;
        bool startNewIteration = false;
        for (auto kvIter = v.begin(); kvIter != v.end(); std::advance(kvIter, 1))
        {
            for (const auto& key : alreadyUsedKeys)
            {
                auto& kv = *kvIter;
                if (CompareByteArrays(key.get(), kv.GetKey(), keyLength))
                {
                    KeyType goodKey = std::make_unique<DB36_NS::Byte[]>(keyLength);
                    memcpy(goodKey.get(), kv.GetKey(), keyLength);
                    alreadyUsedKeys.emplace_back(std::move(goodKey));
                }
                else
                {
                    std::cout << "Found duplicate key" << std::endl;
                    kv.GenerateRandomKey();
                    std::cout << "Generated new random key" << std::endl;
                    startNewIteration = true;
                    break;
                }
            }
            if(startNewIteration)
                break;

            if (kvIter == (v.end() - 1))
                iteratedThroughAllVector = true;

            if (std::distance(kvIter, v.begin()) > 100000 && std::distance(kvIter, v.begin()) % 100000)
            {
                std::cout << "Checked " << std::distance(kvIter, v.begin()) << " keys" << std::endl;
            }
        }
    }
}

// returns vector of time in microseconds
std::vector<unsigned long long> WriteVectorToBlob (std::vector<KeyValuePair>& v, DB36_NS::Blob& blob)
{
    using namespace std::chrono;
    using namespace std::chrono_literals;

    if (v.size() == 0)
        return {};

    if (blob.KeyLength() != v[0].GetKeyLength())
        throw std::logic_error ("Key length in input vector and blob are not equal");

    FindAndReplaceAllNonUniqueKeysInVector(v);

    std::vector<unsigned long long> writeTimesStartMicroseconds;
    for (const auto& kv : v)
    {
        const auto writeTimeStart = system_clock::now();
        blob.Set(kv.GetKey(), kv.GetValue(), kv.GetValueLength());
        writeTimesStartMicroseconds.push_back(duration_cast<microseconds>(system_clock::now() - writeTimeStart).count());
    }
    return writeTimesStartMicroseconds;
}

std::vector<unsigned long long> ReadFromBlobAndCheckKeyValuePairs(const std::vector<KeyValuePair>& v, const DB36_NS::Blob& blob)
{
    using namespace std::chrono;
    using namespace std::chrono_literals;

    if (v.size() == 0)
        return {};

    if (blob.KeyLength() != v[0].GetKeyLength())
        throw std::logic_error ("Key length in input vector and blob are not equal");

    std::vector<unsigned long long> readTimesStartMicroseconds;
    for (const auto& kv : v)
    {
        const auto readTimeStart = system_clock::now();
        const auto data = blob.Get(kv.GetKey());
        readTimesStartMicroseconds.push_back(duration_cast<microseconds>(system_clock::now() - readTimeStart).count());

        if (!CompareByteArrays(data.get(), kv.GetValue(), kv.GetValueLength()))
            throw std::logic_error("KV pair doesn't equal to data");
    }
    return readTimesStartMicroseconds;
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
    auto largeVector = GenerateRandomKeyValuesVector(vectorLength, keyLength, valueLength);
    std::cout << "Making sure there are no duplicate keys" << std::endl;
    FindAndReplaceAllNonUniqueKeysInVector(largeVector);

    Blob b ("/tmp/testblobs/blob.bl", keyLength, valueLength, capacity);

    std::cout << "keyLength\t" << keyLength << std::endl;
    std::cout << "valueLength\t" << valueLength << std::endl;
    std::cout << "capacity\t" << capacity << std::endl;
    std::cout << "vectorLength\t" << vectorLength << std::endl;
    std::cout << "Test started" << std::endl;

    const auto writeTimesMicrosendsVector = WriteVectorToBlob(largeVector, b);
    const auto readTimesMicrosendsVector = ReadFromBlobAndCheckKeyValuePairs(largeVector, b);
   
    std::cout << "Test finished" << std::endl;
    std::cout << "duration\t" << std::accumulate(writeTimesMicrosendsVector.begin(), writeTimesMicrosendsVector.end(), std::accumulate(readTimesMicrosendsVector.begin(), readTimesMicrosendsVector.end(), 0))/1000 << "ms\n";
    std::cout << "average write\t" << double(std::accumulate(writeTimesMicrosendsVector.begin(), writeTimesMicrosendsVector.end(), 0)/writeTimesMicrosendsVector.size())/1000 << "ms\n";
    std::cout << "maximal write\t" << double(*std::max_element(writeTimesMicrosendsVector.begin(), writeTimesMicrosendsVector.end()))/1000 << "ms\n";
    std::cout << "minimal write\t" << double(*std::min_element(writeTimesMicrosendsVector.begin(), writeTimesMicrosendsVector.end()))/1000 << "ms\n";
    std::cout << "average read\t" << double(std::accumulate(readTimesMicrosendsVector.begin(), readTimesMicrosendsVector.end(), 0)/readTimesMicrosendsVector.size())/1000 << "ms\n";
    std::cout << "maximal read\t" << double(*std::max_element(readTimesMicrosendsVector.begin(), readTimesMicrosendsVector.end()))/1000 << "ms\n";
    std::cout << "minimal read\t" << double(*std::min_element(readTimesMicrosendsVector.begin(), readTimesMicrosendsVector.end()))/1000 << "ms\n";

    return 0;
}