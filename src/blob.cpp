#include "blob.h"

#include <cmath>
#include <cstring>
#include <filesystem>
#include <exception>
#include <vector>

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
    auto returnArray = std::make_unique<Byte[]>(len);
    pread(fileno(file.get()), returnArray.get(), len, address);
    return returnArray;
}

uint64_t Blob::WriteBytesToBlob(const uint64_t &address, Byte* data, const uint64_t &len)
{
    pwrite(fileno(file.get()), data, len, address);
    return address + len;
}

uint64_t Blob::GetKeyAddressInShrinkedBlob(const Byte* key) const
{
    const auto startAddress = GetKeyAddress(key);
    uint64_t iter = 0;
    while (!CompareByteKeys(key, ReadBytesFromBlob(startAddress + iter * blobRecordLength, blobKeyLength).get()))
    {
        ++iter;
        if (startAddress + iter * blobRecordLength > blobCapacitySize)
            throw std::logic_error("record not found");
    }
    return startAddress + iter * blobRecordLength;
}

uint64_t Blob::SetKeyAddressInShrinkedBlob(const Byte *key) const
{
    const auto startAddress = GetKeyAddress(key);
    uint64_t iter = 0;
    auto iterKey = std::make_unique<Byte[]>(blobKeyLength);
    const std::unique_ptr<Byte[]> zeros (new Byte[blobKeyLength]{});
    do
    {
        std::memcpy (iterKey.get(), ReadBytesFromBlob(startAddress + iter * blobRecordLength, blobKeyLength).get(), blobKeyLength);
        ++iter;
        if (startAddress + iter * blobRecordLength > blobCapacitySize)
            throw std::logic_error("couldn't set address in shrinked blob");
        
    } while (!(CompareByteKeys(key, iterKey.get()) || CompareByteKeys(iterKey.get(), zeros.get())));
    return startAddress + (iter - 1) * blobRecordLength;
}

// TODO: Return type?
void Blob::Set(Byte* key, Byte* value, const uint64_t& valueLen)
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
    const auto address = SetKeyAddressInShrinkedBlob(key);
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
