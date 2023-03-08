#include <list>
#include <cmath>
#include <string>
#include <fstream>

#include <boost/multiprecision/cpp_int.hpp>


namespace DB36_NS
{

    using BigInt = boost::multiprecision::cpp_int;
    using Byte = uint8_t;
    using ByteList = std::list<Byte>;
    using ByteVector = std::vector<Byte>;

    class Blob 
    {
        private:
            std::string path;
            uint64_t keyLength;
            uint64_t valueLength;
            uint8_t capacity;
            uint64_t recordLength;
            uint64_t capacitySize;
            uint16_t shift;
            uint64_t recordsCount;
            bool isShrinked;
            mutable std::fstream file;
        public:
            uint64_t SlotOf(const BigInt& key) const;
            void ReadAt(const uint64_t& address, ByteList& data);
            void ReadAt(const uint64_t& address, ByteVector& data);

            void WriteAt(const uint64_t& address, const ByteList& data) const;
            void WriteAt(const uint64_t& address, const ByteVector& data) const;

        private:
            void Set(BigInt& key, const ByteList& value);
            ByteList Get(BigInt& key);
        public:
            int64_t RecordsCount() const
            {
                return recordsCount;
            }
            int64_t CapacitySize() const
            {
                return capacitySize;
            }   
            void Init();
            void CreateBlobFile();

            void Destroy();
            void Close();
    };
}