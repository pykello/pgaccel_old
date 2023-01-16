#pragma once

#include "executor.h"

namespace pgaccel
{

struct ColumnDataGroups {
    Rows labels;
    uint16_t groups[1 << 16];
};

class AggState {};
typedef std::unique_ptr<AggState> AggStateP;
typedef std::vector<AggStateP> AggStateVec;

class Aggregator {
public:
    virtual AggStateVec LocalAggregate(const RowGroup& rowGroup,
                                       const ColumnDataGroups& groups) = 0;
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
                                       const ColumnDataGroups& groups);
    virtual void Combine(AggState *result1, const AggState *result2);
    virtual std::string Finalize(const AggState *result);

private:
    bool useAvx;
};

struct SumAggState: public AggState {
    SumAggState(int64_t value = 0): value(value) { }

    int64_t value;
};

class SumAgg: public Aggregator {
public:
    SumAgg(const ColumnRef &columnRef, bool useAvx):
        useAvx(useAvx),
        columnRef(columnRef) { }

    virtual AggStateVec LocalAggregate(const RowGroup& rowGroup,
                                       const ColumnDataGroups& groups);
    virtual void Combine(AggState *result1, const AggState *result2);
    virtual std::string Finalize(const AggState *result);

private:
    bool useAvx;
    ColumnRef columnRef;
};

struct LocalAggResult {
    std::map<Row, std::vector<AggStateP>> groupAggStates;
};

class AggregateNode {
public:
    AggregateNode(const std::vector<AggregateClause> &aggregateClauses,
                  const std::vector<ColumnRef> &groupBy,
                  bool useAvx);

    LocalAggResult ProcessRowGroup(const RowGroup &rowGroup) const;
    void Combine(LocalAggResult &left, LocalAggResult &&right) const;
    Rows Finalize(const LocalAggResult &localResult) const;

    Row FieldNames() const;

private:
    std::vector<AggregatorP> aggregators;
    std::vector<ColumnRef> groupBy;
    std::vector<int> projection;
    Row fieldNames;
};

typedef std::unique_ptr<AggregateNode> AggregateNodeP;

};
