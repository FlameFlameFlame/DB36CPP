#include <benchmark/benchmark.h>

namespace BM
{

union TestUnion
{
    uint8_t bytesArray[8];
    uint64_t number;
};

static void unionTest(benchmark::State& state)
{
    for (auto _ : state)
    {
        for (int i = 0; i < 10000000; ++i)
        {
            TestUnion tu = {1, 2, 3, 4, 5, 6, 7, 0};
            ++tu.number;
        }
    }
}

static void staticCastTest(benchmark::State& state)
{
    for (auto _ : state)
    {
        for (int i = 0; i < 10000000; ++i)
        {
            uint8_t bytesArray[8] = {1, 2, 3, 4, 5, 6, 7, 0};
            uint64_t number = static_cast<uint64_t>(*bytesArray);
            ++number;
        }

    }
}
}

BENCHMARK(BM::staticCastTest);
BENCHMARK(BM::unionTest);

BENCHMARK_MAIN();
