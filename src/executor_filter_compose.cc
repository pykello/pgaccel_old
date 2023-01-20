#include "executor.h"

namespace pgaccel
{

class AndFilterNode: public FilterNodeImpl {
public:
    AndFilterNode(std::vector<FilterNodeP> &&children):
        children(std::move(children)) {}

    virtual int ExecuteCount(const RowGroup &rowGroup) const
    {
        uint8_t bitmask[1 << 13];
        return ExecuteSet(rowGroup, bitmask);
    }

    virtual int ExecuteSet(const RowGroup &rowGroup, uint8_t *bitmask) const
    {
        int result = 0;
        bool first = true;

        for (const auto &child: children)
        {
            if (first)
                result = child->ExecuteSet(rowGroup, bitmask);
            else
                result = child->ExecuteAnd(rowGroup, bitmask);
            first = false;
        }

        return result;
    }

    virtual int ExecuteAnd(const RowGroup &rowGroup, uint8_t *bitmask) const
    {
        int result = 0;

        for (const auto &child: children)
            result = child->ExecuteAnd(rowGroup, bitmask);

        return result;
    }

private:
    std::vector<FilterNodeP> children;
};

FilterNodeP
FilterNodeImpl::CreateAndNode(std::vector<FilterNodeP>&& children)
{
    return std::make_unique<AndFilterNode>(std::move(children));
}

FilterNodeP
CreateFilterNode(const std::vector<FilterClause> &filterClauses_, bool useAvx)
{
    if (filterClauses_.size() == 0)
        return nullptr;

    auto filterClauses = filterClauses_;

    std::vector<FilterNodeP> filterNodes;

    sort(filterClauses.begin(), filterClauses.end(),
         [](const FilterClause &a, const FilterClause &b) -> bool
         {
            if (a.columnRef != b.columnRef)
                return a.columnRef < b.columnRef;
            return a.op < b.op;
         });

    for (int i = 0; i < filterClauses.size(); i++)
    {
        if (i + 1 < filterClauses.size() &&
            filterClauses[i + 1].columnRef == filterClauses[i].columnRef &&
            (filterClauses[i].op == FilterClause::FILTER_GT ||
             filterClauses[i].op == FilterClause::FILTER_GTE) &&
            (filterClauses[i + 1].op == FilterClause::FILTER_LT ||
             filterClauses[i + 1].op == FilterClause::FILTER_LTE))
        {
            filterNodes.push_back(
                FilterNodeImpl::CreateSimpleCompare(
                    filterClauses[i].columnRef,
                    filterClauses[i].value,
                    filterClauses[i].op,
                    filterClauses[i + 1].value,
                    filterClauses[i + 1].op,
                    useAvx
                )
            );

            i++;
        }
        else
        {
            filterNodes.push_back(
                FilterNodeImpl::CreateSimpleCompare(
                    filterClauses[i].columnRef,
                    filterClauses[i].value,
                    filterClauses[i].op,
                    "", FilterClause::INVALID,
                    useAvx));
        }
    }

    if (filterNodes.size() == 1)
        return std::move(filterNodes[0]);

    return FilterNodeImpl::CreateAndNode(std::move(filterNodes));
}

};
