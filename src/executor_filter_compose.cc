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

};
