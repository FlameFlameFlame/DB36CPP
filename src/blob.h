#include <list>
#include <cmath>
#include <string>
#include <memory>
#include <fstream>

#include <boost/multiprecision/cpp_int.hpp>


namespace DB36_NS
{

    using BigInt = boost::multiprecision::cpp_int;
    using Byte = uint8_t;

    class Blob 
    {
        private:
            std::string blobPath;           // absolue path to the blob file
            uint64_t blobKeyLength;         // length of key in bytes
            uint64_t blobValueLength;       // length of value in bytes
            uint8_t blobCapacity;           // capacity is a special parameter: pow (2, capacity) = blobCapacitySize
            uint64_t blobRecordLength;      // record is value + key, if key is stored
            uint64_t blobCapacitySize;      // blob size in bytes
            uint16_t shift = 0;             // shift used in key compression algorythm is blob is shrinked
            uint64_t blobRecordsCount;      // number of records is blob

            bool isShrinked;
            mutable std::fstream file;
        private:
            // calculate slot for the shrinked blob
            uint64_t SlotOf(const BigInt& key) const;
            // find slot for the key in shrinked blob
            uint64_t FindKeySlotInShrinkedBlob(const BigInt& key) const;
            // read bytes in Byte array from the address, adress is in bytes
            std::unique_ptr<Byte[]> ReadBytesFromBlob(const uint64_t& address, const uint64_t& len) const;
            // write bytes from the Byte array to the address, adress is in bytes
            uint64_t WriteBytesToBlob(const uint64_t& address, const Byte* data, const uint64_t& len);
            // create file i
            void CreateBlobFile();
        public:
            // write value associated with the key
            void Set(const BigInt& key, const Byte* value, const uint64_t& valueLen);
            // get value associated with the key
            std::unique_ptr<Byte[]> Get(const BigInt& key) const;
        public:
            int64_t RecordsCount() const
            {
                return blobRecordsCount;
            }
            int64_t CapacitySize() const
            {
                return blobCapacitySize;
            }   
            void Init();
            void Destroy();
            void Close();
    };
}