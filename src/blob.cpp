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

std::unique_ptr<Byte[]> Blob::ReadBytesFromBlob(const uint64_t &address, const uint64_t &len) const
{
    std::unique_ptr<Byte[]> returnArray = std::make_unique<Byte[]>(len); 
    file.seekg(address, std::ios_base::beg);
    for (int i = 0; i < len; ++i)
    {
        file >> returnArray[i]; 
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

uint64_t Blob::FindKeySlotInShrinkedBlob(const BigInt &key) const
{
    const auto keySlot = SlotOf(key);
    const auto address = keySlot * blobRecordLength;
    uint64_t iter = 0;
    BigInt iterKey;
    do 
    {
        memcpy (&iterKey, ReadBytesFromBlob(address + blobRecordLength, blobKeyLength).get(), blobKeyLength);
        if (iter + keySlot > blobCapacity)
            throw std::logic_error("record not found");
    } while (iterKey != key);
    return keySlot + iter;
}

// TODO: Return type?
void Blob::Set(const BigInt& key, const Byte* value, const uint64_t& valueLen)
{
    if (valueLen > blobValueLength)
    {
        throw(std::length_error("record value exceeds size"));
    }

    if (!isShrinked)
    {
        WriteBytesToBlob(SlotOf(key), value, valueLen - blobValueLength);
        return;
    }

    // if we're here, then blob is shrinked
    const auto keySlot = FindKeySlotInShrinkedBlob(key);
    const auto address = keySlot * blobRecordLength;
    {
        auto keyData = std::make_unique<Byte[]>(blobKeyLength);
        memcpy(keyData.get(), &key, blobKeyLength);
        WriteBytesToBlob(keySlot * address, keyData.get(), blobKeyLength);
    }
    {
        WriteBytesToBlob(address + blobKeyLength, value, valueLen);
    }
}


// TODO: return type
std::unique_ptr<Byte[]> Blob::Get(const BigInt& key) const
{
    if (!isShrinked)
        return ReadBytesFromBlob(SlotOf(key), blobValueLength);
    // if we're here, blob is shrinked
    const auto keySlot = FindKeySlotInShrinkedBlob(key);
    return ReadBytesFromBlob(keySlot + blobKeyLength, blobValueLength);
}


void Blob::Init()
{
    if (blobCapacity == 0)
    {
        blobRecordsCount = pow(blobKeyLength * 8, 2.0);
        blobRecordLength = blobValueLength;
        isShrinked = false;
    }
    else
    {
        blobRecordsCount = pow(blobCapacity, 2.0);
        blobRecordLength = blobKeyLength + blobValueLength;
        isShrinked = true;
        shift = blobKeyLength * 8 - blobCapacity;
    }

    blobCapacitySize = blobRecordLength * blobRecordsCount;
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
    
    posix_fallocate(fileno(fileDescriptor), 0, blobCapacitySize);

    std::fclose(fileDescriptor);

    file.open(path, file.in | file.out);
    if (!file.is_open())
        throw(std::logic_error("Failed to initialize blob"));

    if (std::filesystem::file_size(path) != blobCapacitySize)
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
