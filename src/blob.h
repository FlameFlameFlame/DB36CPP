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
            std::string path;
            uint64_t blobKeyLength;
            uint64_t blobValueLength;
            uint8_t blobCapacity;
            uint64_t blobRecordLength;
            uint64_t blobCapacitySize;
            uint16_t shift = 0;
            uint64_t blobRecordsCount;

            bool isShrinked;
            mutable std::fstream file;
        private:
            uint64_t SlotOf(const BigInt& key) const;

            std::unique_ptr<Byte[]> ReadBytesFromBlob(const uint64_t& address, const uint64_t& len) const;
            uint64_t WriteBytesToBlob(const uint64_t& address, const Byte* data, const uint64_t& len);

            std::unique_ptr<Byte[]> GetValueFromShrinkedBlob(const BigInt& key) const;
            std::unique_ptr<Byte[]> GetValueFromUnhrinkedBlob(const BigInt& key) const;

            uint64_t FindKeySlotInShrinkedBlob(const BigInt& key) const;
            
            void CreateBlobFile();
        public:
            void Set(const BigInt& key, const Byte* data, const uint64_t& len);
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