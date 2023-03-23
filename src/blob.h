#include <cerrno>
#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdio.h>
#include <string>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest_prod.h>

namespace DB36_NS
{

    using Byte = uint8_t;

    class Blob 
    {
        private:
            const std::string blobPath;     // absolue path to the blob file
            uint64_t blobKeyLength;         // length of key in bytes
            uint64_t blobValueLength;       // length of value in bytes
            uint8_t blobCapacity;           // capacity is a special parameter: pow (2, capacity) = blobCapacitySize
            uint64_t blobRecordLength;      // record is value + key, if key is stored
            uint64_t blobCapacitySize;      // blob size in bytes
            uint16_t shift = 0;             // shift used in key compression algorythm is blob is shrinked
            uint64_t blobRecordsCount;      // number of records is blob

            bool isShrinked = false;
            std::unique_ptr<FILE, decltype(&fclose)> file;
        protected:
            // calculate address for the shrinked blob
            uint64_t GetKeyAddress(const Byte* key) const;
            // find address for the key in shrinked blob
            uint64_t GetKeyAddressInShrinkedBlob(const Byte* key) const;
            // find address for the key in shrinked blob
            uint64_t SetKeyAddressInShrinkedBlob(const Byte* key) const;
            // read bytes in Byte array from the address, adress is in bytes
            std::unique_ptr<Byte[]> ReadBytesFromBlob(const uint64_t& address, const uint64_t& len) const;
            // write bytes from the Byte array to the address, adress is in bytes
            uint64_t WriteBytesToBlob(const uint64_t& address, Byte* data, const uint64_t& len);
            // create file i
            void CreateBlobFile();
            // convert convert key in byte form to key in uint64 form
            uint64_t ConvertByteKeyToUintKey(const Byte* key) const;

            bool CompareByteKeys(const Byte* key1, const Byte* key2) const;
        public:
            // constructor
            Blob( 
                const std::string& path,
                const uint64_t& keyLength,
                const uint64_t& valueLength,
                const uint8_t& capacity) :
                blobPath(path),
                blobKeyLength(keyLength),
                blobValueLength(valueLength),
                blobCapacity(capacity),
                file(fopen(blobPath.c_str(), "w+"), &fclose)
                {
                    if (!file.get())
                    {

                        std::cerr << "File creation failed: " << std::strerror(errno) << '\n';
                        throw(std::logic_error("Failed to initialize blob"));
                    }
                    posix_fallocate(fileno(file.get()), 0, blobCapacitySize);
                    Init();
                }
            Blob() = delete;
            Blob(const Blob&) = delete;
            Blob& operator= (const Blob&) = delete;
            Blob(Blob&&) = default;
            Blob& operator= (Blob&&) = default;
            ~Blob() = default;
            // write value associated with the key
            void Set(Byte* key, Byte* value, const uint64_t& valueLen);
            // get value associated with the key
            std::unique_ptr<Byte[]> Get(const Byte* key) const;
        public:
            int64_t RecordsCount() const
            {
                return blobRecordsCount;
            }
            int64_t CapacitySize() const
            {
                return blobCapacitySize;
            }
            int64_t KeyLength() const 
            {
                return blobKeyLength;
            }
            int64_t ValueLength() const 
            {
                return blobValueLength;
            }
            void Init();
        private:
            FRIEND_TEST(BlobTest, SlotOfTest);
            FRIEND_TEST(BlobTest, AutoCapacityTest);
            FRIEND_TEST(BlobTest, ReadWriteTest);
    };
}