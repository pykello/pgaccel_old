#pragma once

#include "executor.h"

namespace pgaccel
{

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
                                       uint8_t *bitmap) = 0;
    virtual void Combine(AggState *result1, const AggState *result2) = 0;
    virtual std::string Finalize(const AggState *result) = 0;
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
                                       uint8_t *bitmap);
    virtual void Combine(AggState *result1, const AggState *result2);
    virtual std::string Finalize(const AggState *result);

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
                                       uint8_t *bitmap);
    virtual void Combine(AggState *result1, const AggState *result2);
    virtual std::string Finalize(const AggState *result);

private:
    bool useAvx;
    ColumnRef columnRef;
};

struct LocalAggResult {
    std::map<Row, std::vector<AggStateP>> groupAggStates;
};

typedef std::unique_ptr<LocalAggResult> LocalAggResultP;

class AggregateNodeImpl {
public:
    AggregateNodeImpl(const std::vector<AggregateClause> &aggregateClauses,
                  const std::vector<ColumnRef> &groupBy,
                  FilterNodeP &&filterNode,
                  bool useAvx);

    LocalAggResult ProcessRowGroup(const RowGroup &rowGroup,
                                   uint8_t *selectionBitmap = nullptr) const;
    void Combine(LocalAggResult &left, LocalAggResult &&right) const;
    Rows Finalize(const LocalAggResult &localResult) const;

    Row FieldNames() const;

private:
    std::vector<AggregatorP> aggregators;
    std::vector<ColumnRef> groupBy;
    std::vector<int> projection;
    Row fieldNames;
    FilterNodeP filterNode;
    bool useAvx;
};

typedef std::unique_ptr<AggregateNodeImpl> AggregateNodeP;

};
