#pragma once
#include "executor_value.hpp"
#include <memory_resource>
namespace pgaccel
{
namespace
{
struct Budget : std::pmr::memory_resource
{
    std::atomic<uint64_t> used{0}, peak{0};
    uint64_t limit;
    explicit Budget(uint64_t bytes) : limit(bytes) {}
    void charge(uint64_t n)
    {
        auto old = used.load();
        do {
            if (n > limit || old > limit - n)
                throw std::runtime_error("Query memory budget exceeded (exact results require more memory)");
        } while (!used.compare_exchange_weak(old, old + n));
        auto high = peak.load();
        while (high < old + n && !peak.compare_exchange_weak(high, old + n)) {}
    }
    void *do_allocate(size_t bytes, size_t alignment) override
    {
        charge(bytes);
        try { return std::pmr::new_delete_resource()->allocate(bytes, alignment); }
        catch (...) { used.fetch_sub(bytes); throw; }
    }
    void do_deallocate(void *p, size_t bytes, size_t alignment) override
    {
        std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
        used.fetch_sub(bytes);
    }
    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    { return this == &other; }
};
struct Scratch
{
    Budget &budget;
    uint64_t bytes;
    Scratch(Budget &b, uint64_t n) : budget(b), bytes(n) { budget.charge(bytes); }
    ~Scratch() { budget.used.fetch_sub(bytes); }
};
using Set = std::pmr::unordered_set<Cell, Hash>;
struct State
{
    uint64_t count = 0;
    Wide sum = 0;
    Cell extreme;
    std::shared_ptr<Set> distinct;
};
using States = std::pmr::vector<State>;
using Groups = std::pmr::unordered_map<Cell, States, Hash>;
void Insert(State &state, Cell v, Budget &budget)
{
    if (!state.distinct)
    {
        state.distinct = std::allocate_shared<Set>(std::pmr::polymorphic_allocator<Set>(&budget));
    }
    if (state.distinct->find(v) == state.distinct->end())
    {
        state.distinct->insert(v);
    }
}
void Update(State &state, const AggregateClause &a, const RowGroup &rg, int row, Budget &budget)
{
    if (a.type == AggregateClause::AGGREGATE_PROJECT)
        return;
    if (a.type == AggregateClause::AGGREGATE_COUNT)
    {
        ++state.count;
        return;
    }
    Cell v = Evaluate(*a.expression, rg, row);
    if (a.type == AggregateClause::AGGREGATE_COUNT_DISTINCT)
        Insert(state, v, budget);
    else if (a.type == AggregateClause::AGGREGATE_SUM || a.type == AggregateClause::AGGREGATE_AVG)
        state.sum = Add(state.sum, v.number);
    else if (!state.count || (a.type == AggregateClause::AGGREGATE_MIN ? Less(v, state.extreme)
                                                                       : Less(state.extreme, v)))
        state.extreme = v;
    ++state.count;
}
void Merge(States &to, States &from, const QueryDesc &q, Budget &budget)
{
    for (size_t i = 0; i < to.size(); ++i)
    {
        auto &a = to[i], &b = from[i];
        auto kind = q.aggregateClauses[i].type;
        if (b.distinct)
        {
            if (!a.distinct)
                a.distinct = std::move(b.distinct);
            else
                for (const auto &v : *b.distinct)
                    Insert(a, v, budget);
        }
        if (b.count &&
            (!a.count || (kind == AggregateClause::AGGREGATE_MIN ? Less(b.extreme, a.extreme)
                                                                 : Less(a.extreme, b.extreme))))
            a.extreme = b.extreme;
        a.sum = Add(a.sum, b.sum);
        a.count += b.count;
    }
}
template <class F> void Selected(const RowGroup &rg, const uint8_t *bitmap, F f)
{
    if (!bitmap)
    {
        for (int i = 0; i < rg.size; ++i)
            f(i);
        return;
    }
    for (int base = 0; base < rg.size; base += 64)
    {
        uint64_t bits = 0;
        int n = std::min(64, rg.size - base);
        memcpy(&bits, bitmap + base / 8, (n + 7) / 8);
        if (n < 64)
            bits &= (uint64_t(1) << n) - 1;
        while (bits)
        {
            int bit = __builtin_ctzll(bits);
            f(base + bit);
            bits &= bits - 1;
        }
    }
}
States &Get(Groups &groups, Cell key, size_t n, Budget &budget)
{
    auto it = groups.find(key);
    if (it != groups.end())
        return it->second;
    return groups.emplace(key, States(n, &budget)).first->second;
}
struct FinalCell
{
    Cell value;
    const AccelType *type = nullptr;
    bool null = false;
    bool average = false;
    long double mean = 0;
};
using FinalRow = std::vector<FinalCell>;
std::string Format(const FinalCell &v)
{
    if (v.null)
        return "NULL";
    if (v.average)
    {
        std::ostringstream s;
        s << std::setprecision(17) << v.mean;
        return s.str();
    }
    if (v.value.string)
        return std::string(v.value.text);
    if (v.type && (v.type->type_num() == DATE_TYPE || v.type->type_num() == TIMESTAMP_TYPE))
        return ToString(v.type, int64_t(v.value.number));
    return Number(v.value.number, v.type ? Scale(v.type) : 0);
}
} // namespace
} // namespace pgaccel
