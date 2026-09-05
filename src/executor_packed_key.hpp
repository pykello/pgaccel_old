#pragma once
#include "executor_state.hpp"
#include "column_index.hpp"
#include <bit>

namespace pgaccel {
namespace {
// Exact bounded identities, not truncated hashes. Unsupported or wider keys
// retain the normal logical comparison path.
class PackedGroupKey {
    enum Kind { ID, NUMBER, MINUTE, HOUR, SECOND, TRUNC_MINUTE, TRUNC_HOUR, TRUNC_DAY, CONDITIONAL_ID };
    struct Frame {
        const uint8_t *values = nullptr;
        const void *mapping = nullptr;
        int width = 0;
        bool dictionary = false;
    };
    struct Component {
        Kind kind;
        unsigned bits, shift;
        int64_t minimum;
        const Expression *condition;
        uint32_t fallback;
        bool sourceOnTrue;
        std::pmr::vector<Frame> frames;
        Component(Budget &memory, Kind k, unsigned b, unsigned s, int64_t m,
                  const Expression *c, uint32_t f, bool t)
            : kind(k), bits(b), shift(s), minimum(m), condition(c), fallback(f), sourceOnTrue(t), frames(&memory) {}
    };
    std::pmr::vector<Component> components;
    bool valid_ = false;
    unsigned bits_ = 0;
    const ColumnarTable *table_;
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
    template <int Unit> static int64_t quotient(int64_t seconds) {
        int64_t value = seconds / Unit;
        return seconds < 0 && seconds % Unit ? value - 1 : value;
    }
    template <int Unit, int Modulus> static uint64_t part(int64_t seconds) {
        int64_t quotient = seconds / Unit;
        if (seconds < 0 && seconds % Unit) --quotient;
        int64_t value = quotient % Modulus;
        return value < 0 ? value + Modulus : value;
    }
  public:
    PackedGroupKey(const QueryDesc &q, const std::vector<std::shared_ptr<Expression>> &identity,
                   Budget &memory) : components(&memory), table_(q.tables[0]) {
        struct Plan { Kind kind; int column; unsigned bits, shift; const GlobalDictionary *index;
            int64_t minimum = 0; const Expression *condition = nullptr; uint32_t fallback = 0; bool sourceOnTrue = true; };
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
                    int64_t minimum = INT64_MAX, maximum = INT64_MIN;
                    for (int g = 0; g < table.RowGroupCount(); ++g) {
                        auto data = table.GetRowGroup(g).columns[plan.column].get();
                        if (!data->size) continue;
                        int64_t lo, hi;
                        if (data->type == ColumnDataBase::DICT_COLUMN_DATA) {
                            auto dict = static_cast<const DictColumnData<DateType> *>(data);
                            lo = dict->dict.front(); hi = dict->dict.back();
                        } else {
                            auto bounds = [&]<class T>() {
                                auto raw = static_cast<const RawColumnData<T> *>(data);
                                lo = raw->minValue; hi = raw->maxValue;
                            };
                            switch (expression->type->type_num()) {
                            case INT32_TYPE: bounds.template operator()<Int32Type>(); break;
                            case INT64_TYPE: bounds.template operator()<Int64Type>(); break;
                            case TIMESTAMP_TYPE: bounds.template operator()<TimestampType>(); break;
                            case DECIMAL_TYPE: bounds.template operator()<DecimalType>(); break;
                            case DATE_TYPE: bounds.template operator()<DateType>(); break;
                            default: return;
                            }
                        }
                        minimum = std::min(minimum, lo); maximum = std::max(maximum, hi);
                    }
                    plan.minimum = minimum;
                    plan.bits = minimum <= maximum ? std::bit_width(uint64_t(maximum) - uint64_t(minimum)) : 0;
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
            } else if (expression->kind == Expression::DATE_TRUNC &&
                       expression->left->kind == Expression::COLUMN &&
                       expression->left->type->type_num() == TIMESTAMP_TYPE) {
                plan.column = expression->left->column->columnIdx;
                int unit;
                if (expression->literal == "minute") { plan.kind = TRUNC_MINUTE; unit = 60; }
                else if (expression->literal == "hour") { plan.kind = TRUNC_HOUR; unit = 3600; }
                else if (expression->literal == "day") { plan.kind = TRUNC_DAY; unit = 86400; }
                else return;
                int64_t minimum = INT64_MAX, maximum = INT64_MIN;
                for (int g = 0; g < table.RowGroupCount(); ++g) {
                    auto data = table.GetRowGroup(g).columns[plan.column].get();
                    if (data->type != ColumnDataBase::RAW_COLUMN_DATA) return;
                    if (!data->size) continue;
                    auto raw = static_cast<const RawColumnData<TimestampType> *>(data);
                    auto floor = [&](int64_t seconds) {
                        int64_t value = seconds / unit;
                        return seconds < 0 && seconds % unit ? value - 1 : value;
                    };
                    minimum = std::min(minimum, floor(raw->minValue));
                    maximum = std::max(maximum, floor(raw->maxValue));
                }
                plan.minimum = minimum;
                plan.bits = minimum <= maximum ? std::bit_width(uint64_t(maximum) - uint64_t(minimum)) : 0;
            } else if (expression->kind == Expression::CASE && expression->type->type_num() == STRING_TYPE) {
                auto source = expression->left, literal = expression->right;
                if (source->kind == Expression::CONSTANT && literal->kind == Expression::COLUMN) {
                    std::swap(source, literal); plan.sourceOnTrue = false;
                }
                if (source->kind != Expression::COLUMN || literal->kind != Expression::CONSTANT ||
                    source->type->type_num() != STRING_TYPE || literal->type->type_num() != STRING_TYPE) return;
                plan.column = source->column->columnIdx;
                plan.index = table.Dictionary(plan.column);
                if (!plan.index) return;
                plan.kind = CONDITIONAL_ID;
                plan.condition = expression->condition.get();
                plan.fallback = plan.index->Find(literal->literal);
                if (plan.fallback == UINT32_MAX) plan.fallback = plan.index->size();
                size_t maximum = std::max(size_t(plan.fallback), plan.index->size() ? plan.index->size() - 1 : size_t(0));
                // Even an equal-branch CASE must evaluate its condition.
                plan.bits = std::max(1u, unsigned(std::bit_width(maximum)));
            } else return;
            if (plan.bits > 64 - total) return;
            total += plan.bits;
            if (plan.bits) plans.push_back(plan);
        }
        for (const auto &plan : plans) {
            components.emplace_back(memory, plan.kind, plan.bits, plan.shift, plan.minimum,
                                    plan.condition, plan.fallback, plan.sourceOnTrue);
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
            case CONDITIONAL_ID:
                value = (bool(Evaluate(*component.condition, table_->GetRowGroup(group), row).number) == component.sourceOnTrue)
                    ? static_cast<const uint32_t *>(frame.mapping)[frame.dictionary ? code(frame, row) : row]
                    : component.fallback;
                break;
            case NUMBER: {
                int64_t number = frame.dictionary
                    ? static_cast<const int32_t *>(frame.mapping)[code(frame, row)] : raw(frame, row);
                value = uint64_t(number) - uint64_t(component.minimum);
                break;
            }
            case MINUTE: value = part<60, 60>(raw(frame, row)); break;
            case HOUR: value = part<3600, 24>(raw(frame, row)); break;
            case SECOND: value = part<1, 60>(raw(frame, row)); break;
            case TRUNC_MINUTE: value = uint64_t(quotient<60>(raw(frame, row))) - uint64_t(component.minimum); break;
            case TRUNC_HOUR: value = uint64_t(quotient<3600>(raw(frame, row))) - uint64_t(component.minimum); break;
            case TRUNC_DAY: value = uint64_t(quotient<86400>(raw(frame, row))) - uint64_t(component.minimum); break;
            }
            result |= value << component.shift;
        }
        return result;
    }
};
} // namespace
} // namespace pgaccel
