#include "blob.h"

#include <cmath>
#include <filesystem>
#include <exception>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace DB36_NS
{

uint64_t Blob::SlotOf(const BigInt& key) const
{
    return static_cast<uint64_t>(key>>shift);
}

void Blob::ReadAt(const uint64_t& address, ByteList& data)
{
    file.seekg(address * recordLength, std::ios_base::beg);
    for (auto& b : data)
    {
        file >> b;
    }
}

void Blob::ReadAt(const uint64_t& address, ByteVector& data)
{
    file.seekg(address * recordLength, std::ios_base::beg);
    for (auto& b : data)
    {
        file >> b;
    }
}

void Blob::WriteAt(const uint64_t& address, const ByteList& data) const
{
    file.seekg(address * recordLength, std::ios_base::beg);
    for (const auto& b : data)
    {
        file << b;
    }
}

void Blob::WriteAt(const uint64_t& address, const ByteVector& data) const
{
    file.seekg(address * recordLength, std::ios_base::beg);
    for (const auto& b : data)
    {
        file << b;
    }
}

// TODO: Return type?
void Blob::Set(BigInt& key, const ByteList& value)
{
    const uint64_t valueLen = value.size();

    ByteVector data(recordLength);
    auto i = SlotOf(key);
    Byte iters;

    if (valueLen > valueLength)
    {
        throw(std::length_error("record value exceeds size"));
    }

    if (!isShrinked)
    {
        auto valueIt = value.begin();
        const auto padding = valueLength - valueLen;
        std::copy(value.begin(), value.end(), data.begin() + padding);
        WriteAt(i, data);
        return;
    }

    ByteList keyData;
    boost::multiprecision::export_bits(key, std::back_inserter(keyData), 8);
    const uint64_t keyDataLen = keyData.size();
    std::copy(keyData.begin(), keyData.end(), data.begin() + keyLength - keyDataLen);
    std::copy(value.begin(), value.end(), data.begin() + recordLength - valueLen);


    BigInt recordKey;
    ByteVector recordKeyData(keyLength);
    
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
    ByteList data(recordLength);
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


void Blob::Init()
{
    shift = keyLength * 8 - capacity;
    if (capacity == 0)
    {
        recordsCount = pow(keyLength * 8, 2.0);
        recordLength = valueLength;
        isShrinked = false;
    }
    else
    {
        recordsCount = pow(capacity, 2.0);
        recordLength = keyLength + valueLength;
        isShrinked = true;
    }

    capacitySize = recordLength * recordsCount;
    if (file.is_open())
        throw(std::logic_error("Blob is already initialized"));

    CreateBlobFile();
}

void Blob::CreateBlobFile()
{
    const auto fs_path = std::filesystem::path(path);
    const auto dir = fs_path.root_name().string() + fs_path.root_directory().string() + fs_path.relative_path().string();

    std::filesystem::create_directory(dir);
    if (!std::filesystem::exists(dir))
        throw(std::logic_error("Failed to initialize directory"));

    FILE* fileDescriptor = std::fopen(path.c_str(), "w+");
    if (!fileDescriptor)
        throw(std::logic_error("Failed to initialize blob"));
    
    posix_fallocate(fileno(fileDescriptor), 0, capacitySize);

    std::fclose(fileDescriptor);

    file.open(path, file.in | file.out);
    if (!file.is_open())
        throw(std::logic_error("Failed to initialize blob"));

    if (std::filesystem::file_size(path) != capacitySize)
        throw(std::logic_error("Wrong size"));

}

void Blob::Destroy()
{
    if (file.is_open())
        throw(std::logic_error("Tried to destroy opened file"));

    std::filesystem::remove(path);

}
void Blob::Close()
{
    file.close();
}
}
