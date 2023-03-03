#include "blob.h"

#include <exception>
#include <vector>

namespace DB36_NS
{

int64_t Blob::SlotOf(BigInt& key)
{
    if (shift > 0)
    {
        key >>= shift;
    }
    return static_cast<uint64_t>(key);
}

void Blob::ReadAt(const int64_t& address, ByteList& data)
{
    file->seekg(address * recordSize, std::ios_base::beg);
    for (auto& b : data)
    {
        *file >> b;
    }
}

void Blob::ReadAt(const int64_t& address, ByteVector& data)
{
    file->seekg(address * recordSize, std::ios_base::beg);
    for (auto& b : data)
    {
        *file >> b;
    }
}

void Blob::WriteAt(const int64_t& address, const ByteList& data) const
{
    file->seekg(address * recordSize, std::ios_base::beg);
    for (const auto& b : data)
    {
        *file << b;
    }
}

void Blob::WriteAt(const int64_t& address, const ByteVector& data) const
{
    file->seekg(address * recordSize, std::ios_base::beg);
    for (const auto& b : data)
    {
        *file << b;
    }
}

// TODO: Return type?
void Blob::Set(BigInt& key, const ByteList& value)
{
    const uint64_t valueLen = value.size();

    ByteVector data(recordSize);
    auto i = SlotOf(key);
    Byte iters;

    if (valueLen > valueSize)
    {
        throw(std::length_error("record value exceeds size"));
    }

    if (!isShrinked)
    {
        auto valueIt = value.begin();
        const auto padding = valueSize - valueLen;
        std::copy(value.begin(), value.end(), data.begin() + padding);
        WriteAt(i, data);
        return;
    }

    ByteList keyData;
    boost::multiprecision::export_bits(key, std::back_inserter(keyData), 8);
    const uint64_t keyDataLen = keyData.size();
    std::copy(keyData.begin(), keyData.end(), data.begin() + keySize - keyDataLen);
    std::copy(value.begin(), value.end(), data.begin() + recordSize - valueLen);


    BigInt recordKey;
    ByteVector recordKeyData(keySize);
    
    do 
    {
        ++iters;
        ReadAt(i, recordKeyData);
        // setbytes
        if (key != recordKey || recordKey == 0)
        {
            WriteAt(i, data);
            return;
        }
        ++i;
    } while (iters <= capacity);
    throw(std::logic_error("record not found"));
}


// TODO: return type
ByteList Blob::Get(BigInt& key)
{
    ByteList data(recordSize);
    auto i = SlotOf(key);
    uint8_t iters;

    BigInt recordKey;
    do 
    {
        ++iters;
        ReadAt(i, data);

        if (!isShrinked)
            return data;

        // setbytes
        if (key != recordKey)
        {
            return data;
        }
        ++i;
    } while (iters <= capacity);
    throw(std::logic_error("record not found"));
}

}
