#pragma once
#include "executor_state.hpp"
#include "column_index.hpp"
#include <bit>

namespace pgaccel {
namespace {
// Exact bounded identities, not truncated hashes. Unsupported or wider keys
// retain the normal logical comparison path.
class PackedGroupKey {
    enum Kind { ID, NUMBER, MINUTE, HOUR, SECOND };
    struct Frame {
        const uint8_t *values = nullptr;
        const void *mapping = nullptr;
        int width = 0;
        bool dictionary = false;
    };
    struct Component {
        Kind kind;
        unsigned bits, shift;
        std::pmr::vector<Frame> frames;
        Component(Budget &memory, Kind k, unsigned b, unsigned s)
            : kind(k), bits(b), shift(s), frames(&memory) {}
    };
    std::pmr::vector<Component> components;
    bool valid_ = false;
    unsigned bits_ = 0;
    static int64_t raw(const Frame &f, int row) {
        switch (f.width) {
        case 1: return reinterpret_cast<const int8_t *>(f.values)[row];
        case 2: return reinterpret_cast<const int16_t *>(f.values)[row];
        case 4: return reinterpret_cast<const int32_t *>(f.values)[row];
        case 8: return reinterpret_cast<const int64_t *>(f.values)[row];
        }
        throw std::runtime_error("Invalid packed key storage width");
    }
    static uint32_t code(const Frame &f, int row) {
        return f.width == 1 ? f.values[row] : reinterpret_cast<const uint16_t *>(f.values)[row];
    }
    template <int Unit, int Modulus> static uint64_t part(int64_t seconds) {
        int64_t quotient = seconds / Unit;
        if (seconds < 0 && seconds % Unit) --quotient;
        int64_t value = quotient % Modulus;
        return value < 0 ? value + Modulus : value;
    }
  public:
    PackedGroupKey(const QueryDesc &q, const std::vector<std::shared_ptr<Expression>> &identity,
                   Budget &memory) : components(&memory) {
        struct Plan { Kind kind; int column; unsigned bits, shift; const GlobalDictionary *index; };
        std::pmr::vector<Plan> plans(&memory);
        unsigned total = 0;
        const auto &table = *q.tables[0];
        for (const auto &expression : identity) {
            Plan plan{NUMBER, 0, 0, total, nullptr};
            if (expression->kind == Expression::COLUMN) {
                plan.column = expression->column->columnIdx;
                plan.index = table.Dictionary(plan.column);
                if (plan.index) {
                    plan.kind = ID;
                    plan.bits = std::bit_width(plan.index->size() ? plan.index->size() - 1 : size_t(0));
                } else {
                    if (expression->type->type_num() == STRING_TYPE) return;
                    for (int g = 0; g < table.RowGroupCount(); ++g) {
                        auto data = table.GetRowGroup(g).columns[plan.column].get();
                        unsigned bits = data->type == ColumnDataBase::DICT_COLUMN_DATA
                            ? 32 : static_cast<const RawColumnDataBase *>(data)->bytesPerValue * 8;
                        plan.bits = std::max(plan.bits, bits);
                    }
                }
            } else if (expression->kind == Expression::EXTRACT && expression->left->kind == Expression::COLUMN &&
                       expression->left->type->type_num() == TIMESTAMP_TYPE) {
                plan.column = expression->left->column->columnIdx;
                if (expression->literal == "minute") { plan.kind = MINUTE; plan.bits = 6; }
                else if (expression->literal == "hour") { plan.kind = HOUR; plan.bits = 5; }
                else if (expression->literal == "second") { plan.kind = SECOND; plan.bits = 6; }
                else return;
                for (int g = 0; g < table.RowGroupCount(); ++g)
                    if (table.GetRowGroup(g).columns[plan.column]->type != ColumnDataBase::RAW_COLUMN_DATA) return;
            } else return;
            if (plan.bits > 64 - total) return;
            total += plan.bits;
            if (plan.bits) plans.push_back(plan);
        }
        for (const auto &plan : plans) {
            components.emplace_back(memory, plan.kind, plan.bits, plan.shift);
            auto &frames = components.back().frames;
            frames.reserve(table.RowGroupCount());
            for (int g = 0; g < table.RowGroupCount(); ++g) {
                auto data = table.GetRowGroup(g).columns[plan.column].get();
                Frame frame;
                frame.dictionary = data->type == ColumnDataBase::DICT_COLUMN_DATA;
                if (frame.dictionary) {
                    auto dictionary = static_cast<const DictColumnDataBase *>(data);
                    frame.values = dictionary->values;
                    frame.width = dictionary->bytesPerValue();
                    if (!plan.index) frame.mapping = static_cast<const DictColumnData<DateType> *>(data)->dict.data();
                } else {
                    auto raw = static_cast<const RawColumnDataBase *>(data);
                    frame.values = raw->values;
                    frame.width = raw->bytesPerValue;
                }
                if (plan.index) frame.mapping = plan.index->Codes(g).data();
                frames.push_back(frame);
            }
        }
        bits_ = total;
        valid_ = true;
    }
    bool valid() const { return valid_; }
    unsigned bits() const { return bits_; }
    uint64_t word(int group, int row) const {
        uint64_t result = 0;
        for (const auto &component : components) {
            const auto &frame = component.frames[group];
            uint64_t value;
            switch (component.kind) {
            case ID:
                value = static_cast<const uint32_t *>(frame.mapping)[frame.dictionary ? code(frame, row) : row];
                break;
            case NUMBER: {
                int64_t number = frame.dictionary
                    ? static_cast<const int32_t *>(frame.mapping)[code(frame, row)] : raw(frame, row);
                value = uint64_t(number) + (uint64_t(1) << (component.bits - 1));
                break;
            }
            case MINUTE: value = part<60, 60>(raw(frame, row)); break;
            case HOUR: value = part<3600, 24>(raw(frame, row)); break;
            case SECOND: value = part<1, 60>(raw(frame, row)); break;
            }
            result |= value << component.shift;
        }
        return result;
    }
};
} // namespace
} // namespace pgaccel
