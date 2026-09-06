#include "executor_wait_summary.hpp"
#include <gtest/gtest.h>
#include <vector>
using namespace pgaccel;
TEST(WaitSummary, MatchesCorrelatedDefinitionAndEverySplitMerge) {
    for(int size=1,power=6;size<=4;++size,power*=6)for(int code=0;code<power;++code) {
        std::vector<std::pair<int64_t,bool>> rows;int rest=code;
        for(int i=0;i<size;++i){rows.push_back({rest%3-1,bool((rest%6)/3)});rest/=6;}
        auto summarize=[&](int begin,int end) {
            WaitSummary summary(-9,rows[begin].first);
            for(int i=begin;i<end;++i)summary.observe(rows[i].first,rows[i].second);
            return summary;
        };
        uint64_t expected=0;
        for(const auto &[supplier,late]:rows)if(late) {
            bool different=false,lateDifferent=false;
            for(const auto &[other,otherLate]:rows)if(other!=supplier){different=true;lateDifferent|=otherLate;}
            expected+=different && !lateDifferent;
        }
        auto whole=summarize(0,size);
        EXPECT_EQ(whole.different && !whole.lateDifferent?whole.lateCount:0,expected);
        for(int split=1;split<size;++split) {
            auto merged=summarize(0,split);merged.merge(summarize(split,size));
            EXPECT_EQ(merged.firstSupplier,whole.firstSupplier);EXPECT_EQ(merged.firstLate,whole.firstLate);
            EXPECT_EQ(merged.lateCount,whole.lateCount);EXPECT_EQ(merged.different,whole.different);EXPECT_EQ(merged.lateDifferent,whole.lateDifferent);
        }
    }
}
