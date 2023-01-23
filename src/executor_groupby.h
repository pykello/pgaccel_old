#pragma once

#include "executor.h"
#include <functional>

namespace pgaccel
{

struct ExecutionParams {
    bool useAvx = true;
    bool groupByEliminateBranches = true;
};

struct ColumnDataGroups {
    int groupCount;
    uint16_t groups[1 << 16] __attribute__ ((aligned (512)));
};

class AggState {};
typedef std::unique_ptr<AggState> AggStateP;
typedef std::vector<AggStateP> AggStateVec;

class Aggregator {
public:
    virtual AggStateVec LocalAggregate(const RowGroup& rowGroup,
                                       const ColumnDataGroups& groups,
                                       uint8_t *bitmap) const = 0;
    virtual void Combine(AggState *result1, const AggState *result2) const = 0;
    virtual std::string Finalize(const AggState *result) const = 0;
};

typedef std::unique_ptr<Aggregator> AggregatorP;

struct CountAggState: public AggState {
    CountAggState(int32_t value = 0): value(value) { }

    int32_t value;
};

class CountAgg: public Aggregator {
public:
    CountAgg(bool useAvx): useAvx(useAvx) { }

    virtual AggStateVec LocalAggregate(const RowGroup& rowGroup,
                                       const ColumnDataGroups& groups,
                                       uint8_t *bitmap) const;
    virtual void Combine(AggState *result1, const AggState *result2) const;
    virtual std::string Finalize(const AggState *result) const;

private:
    bool useAvx;
};

struct SumAggState: public AggState {
    SumAggState(int64_t value,
                std::shared_ptr<AccelType> valueType):
                    value(value),
                    valueType(valueType) { }

    int64_t value;
    std::shared_ptr<AccelType> valueType;
};

class SumAgg: public Aggregator {
public:
    SumAgg(const ColumnRef &columnRef, bool useAvx):
        useAvx(useAvx),
        columnRef(columnRef) { }

    virtual AggStateVec LocalAggregate(const RowGroup& rowGroup,
                                       const ColumnDataGroups& groups,
                                       uint8_t *bitmap) const;
    virtual void Combine(AggState *result1, const AggState *result2) const;
    virtual std::string Finalize(const AggState *result) const;

private:
    bool useAvx;
    ColumnRef columnRef;
};

struct LocalAggResult {
    typedef std::vector<std::shared_ptr<AccelType>> Schema;

    struct TypedCmp {
        TypedCmp(const Schema &schema): schema(schema) {}

        bool operator()(const RowX& a,
                        const RowX& b) const {
            for (int i = 0; i < a.size() && i < b.size(); i++)
                if (schema[i]->type_num() == STRING_TYPE)
                {
                    if (a[i].strValue != b[i].strValue)
                        return a[i].strValue < b[i].strValue;
                }
                else
                {
                    if (a[i].int64Value != b[i].int64Value)
                        return a[i].int64Value < b[i].int64Value;
                }
            return false;
        }

        Schema schema;
    };

    LocalAggResult(const Schema & schema):
        schema(schema),
        groupAggStates(TypedCmp(schema)) {}

    Schema schema;
    std::map<RowX, std::vector<AggStateP>,
             std::function<bool(const RowX&, const RowX&)>> groupAggStates;
};

typedef std::unique_ptr<LocalAggResult> LocalAggResultP;

class AggregateNodeImpl {
public:
    AggregateNodeImpl(const std::vector<AggregateClause> &aggregateClauses,
                     const std::vector<ColumnRef> &groupBy,
                     FilterNodeP &&filterNode,
                     const ExecutionParams &params);

    LocalAggResult ProcessRowGroup(const RowGroup &rowGroup,
                                   uint8_t *selectionBitmap = nullptr) const;
    void Combine(LocalAggResult &left, LocalAggResult &&right) const;
    Rows Finalize(const LocalAggResult &localResult) const;

    LocalAggResult::Schema GroupBySchema() const {
        return groupBySchema;
    }

    Row FieldNames() const;

private:
    std::vector<AggregatorP> aggregators;
    std::vector<ColumnRef> groupBy;
    std::vector<int> projection;
    Row fieldNames;
    FilterNodeP filterNode;
    ExecutionParams params;
    LocalAggResult::Schema groupBySchema;
};

typedef std::unique_ptr<AggregateNodeImpl> AggregateNodeP;

};
