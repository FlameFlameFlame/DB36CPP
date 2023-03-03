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
            uint64_t keySize;
            uint64_t valueSize;
            uint8_t capacity;
            int64_t recordSize;
            int64_t capacitySize;
            uint16_t shift;
            int64_t recordsCount;
            bool isShrinked;
            std::fstream* file;
        public:
            int64_t SlotOf(BigInt& key);
            void ReadAt(const int64_t& address, ByteList& data);
            void ReadAt(const int64_t& address, ByteVector& data);

            void WriteAt(const int64_t& address, const ByteList& data) const;
            void WriteAt(const int64_t& address, const ByteVector& data) const;

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
            void Destroy();
            void Close();
    };
}