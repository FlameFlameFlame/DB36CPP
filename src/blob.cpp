#include "blob.h"

#include <cmath>
#include <cstring>
#include <filesystem>
#include <exception>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace DB36_NS
{

uint64_t Blob::GetKeyAddress(const Byte* key) const
{
    uint64_t retVal = 0;
    if (blobKeyLength > sizeof(uint64_t))
    {
        // copy last sizeof(uint64_t) bytes for key that is longer than uint64_t
        std::memcpy(&retVal, key + (blobKeyLength - sizeof(uint64_t)), sizeof(uint64_t)); 
    }
    else 
    {
        // fill the last blobKeyLength bytes in uint64_t if key is shorter that uint64_t
        std::memcpy(&retVal, key, blobKeyLength);
    }
    return isShrinked ? (retVal >> shift) * blobRecordLength : retVal >> shift;
}

std::unique_ptr<Byte[]> Blob::ReadBytesFromBlob(const uint64_t &address, const uint64_t &len) const
{
    std::unique_ptr<Byte[]> returnArray = std::make_unique<Byte[]>(len); 
    file.seekg(address, std::ios_base::beg);
    for (int i = 0; i < len; ++i)
    {
        file >> returnArray.get()[i]; 
    }
    return returnArray;
}

uint64_t Blob::WriteBytesToBlob(const uint64_t &address, const Byte* data, const uint64_t &len)
{
    file.seekg(address, std::ios_base::beg);
    for (int i = 0; i < len; ++i)
    {
        file << data[i]; 
    }
    return address + len;
}

uint64_t Blob::GetKeyAddressInShrinkedBlob(const Byte* key) const
{
    const auto startAddress = GetKeyAddress(key);
    uint64_t iter = 0;
    auto iterKey = std::make_unique<Byte[]>(blobKeyLength);
    do 
    {
        // read key in iterKey
        std::memcpy (iterKey.get(), ReadBytesFromBlob(startAddress + iter * blobRecordLength, blobKeyLength).get(), blobKeyLength);
        ++iter;
        if (startAddress + iter * blobRecordLength > blobCapacitySize)
            throw std::logic_error("record not found");
    } while (!CompareByteKeys(iterKey.get(), key));
    return startAddress + (iter - 1) * blobRecordLength;
}

// TODO: Return type?
void Blob::Set(const Byte* key, const Byte* value, const uint64_t& valueLen)
{
    if (valueLen > blobValueLength)
    {
        throw(std::length_error("record value exceeds size"));
    }
    if (!isShrinked)
    {
        WriteBytesToBlob(GetKeyAddress(key), value, valueLen);
        return;
    }
    // if we're here, then blob is shrinked
    const auto address = GetKeyAddress(key);
    WriteBytesToBlob(address, key, blobKeyLength);
    WriteBytesToBlob(address + blobKeyLength, value, valueLen);
}


// TODO: return type
std::unique_ptr<Byte[]> Blob::Get(const Byte* key) const
{
    if (!isShrinked)
        return ReadBytesFromBlob(GetKeyAddress(key), blobValueLength);
    // if we're here, blob is shrinked
    const auto address = GetKeyAddressInShrinkedBlob(key);
    return ReadBytesFromBlob(address + blobKeyLength, blobValueLength);
}


void Blob::Init()
{
    if (blobKeyLength * 8 > blobCapacity)
    {
        shift = blobKeyLength * 8 - blobCapacity;

    }
    if (blobCapacity == 0)
    {
        blobRecordsCount = pow(2, blobKeyLength * 8);
        blobRecordLength = blobValueLength;
        isShrinked = false;
    }
    else
    {
        blobRecordsCount = pow(2, blobCapacity);
        blobRecordLength = blobKeyLength + blobValueLength;
        isShrinked = true;
    }
    blobCapacitySize = blobRecordLength * blobRecordsCount;
    if (file.is_open())
        throw(std::logic_error("Blob is already initialized"));

   CreateBlobFile();
}

void Blob::CreateBlobFile()
{
    const auto fsPath = std::filesystem::path(blobPath);
    const auto dir = fsPath.parent_path().string();

    std::filesystem::create_directory(dir);
    if (!std::filesystem::exists(dir))
        throw(std::logic_error("Failed to initialize directory"));

    // open in C-style to call posix_fallocate
    FILE* fileDescriptor = std::fopen(blobPath.c_str(), "w+");
    if (!fileDescriptor)
        throw(std::logic_error("Failed to initialize blob"));
    posix_fallocate(fileno(fileDescriptor), 0, blobCapacitySize);
    std::fclose(fileDescriptor);

    // now open in C++ style
    file.open(blobPath, file.in | file.out);
    if (!file.is_open())
        throw(std::logic_error("Failed to initialize blob"));

    if (std::filesystem::file_size(blobPath) != blobCapacitySize)
        throw(std::logic_error("Wrong size"));

}

void Blob::Destroy()
{
    if (file.is_open())
        throw(std::logic_error("Tried to destroy opened file"));
    std::filesystem::remove(blobPath);
}
void Blob::Close()
{
    file.close();
}

uint64_t Blob::ConvertByteKeyToUintKey(const Byte* key) const
{
    uint64_t tempKey = 0;
    std::memcpy(&tempKey, key, blobKeyLength < sizeof(uint64_t) ? blobKeyLength : sizeof(uint64_t));
    return tempKey;
}

bool Blob::CompareByteKeys(const Byte *key1, const Byte *key2) const
{
    for (int i = 0; i < blobKeyLength; ++i)
    {
        if (key1[i] != key2[i])
            return false;
    }
    return true;
}
}
