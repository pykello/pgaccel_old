#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteSupplierWait(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;
namespace {
TableRegistry WaitTables(int shift=0,bool broken=false,bool duplicate=false) {
    auto integer=std::make_shared<Int64Type>();std::vector<int64_t> orders,suppliers;std::vector<int32_t> receipt,commit;
    auto addRow=[&](int order,int supplier,bool late) {
        orders.push_back(order+shift);suppliers.push_back(supplier);int day=1+orders.size()%7;
        commit.push_back(day);receipt.push_back(day+(late?1:-1));
    };
    addRow(-100,10,true);addRow(-100,10,true);
    addRow(-98,10,true);addRow(-98,11,false);
    addRow(-96,10,true);addRow(-96,11,true);
    addRow(-94,11,true);addRow(-94,10,false);addRow(-94,11,true);
    for(int i=0;i<70;++i)addRow(-92,i==69?12:13,i!=69);
    addRow(-90,12,true);addRow(-90,10,false);
    addRow(-88,10,true);addRow(-88,12,true);addRow(-88,13,false);
    addRow(-86,14,false);addRow(-86,10,true);
    addRow(-84,10,true);addRow(-84,11,false);
    addRow(-82,10,false);addRow(-82,11,false);
    addRow(999,10,true);addRow(999,11,false);
    if(broken){std::swap(orders[2],orders[6]);std::swap(suppliers[2],suppliers[6]);std::swap(receipt[2],receipt[6]);std::swap(commit[2],commit[6]);}
    std::vector<std::vector<ColumnDataP>> frames;
    for(size_t begin=0;begin<orders.size();begin+=17) {
        size_t end=std::min(begin+17,orders.size());
        frames.push_back({test::Raw({orders.begin()+begin,orders.begin()+end},4),test::Raw({suppliers.begin()+begin,suppliers.begin()+end},4),
            test::Dict<DateType>({receipt.begin()+begin,receipt.begin()+end}),test::Dict<DateType>({commit.begin()+begin,commit.begin()+end})});
    }
    std::rotate(frames.begin(),frames.begin()+2,frames.end());
    auto tables=test::Table(std::move(frames),{{"o",integer},{"s",integer},{"receipt",std::make_shared<DateType>()},{"commit",std::make_shared<DateType>()}});
    auto add=[&](std::string name,TableRegistry table){tables.emplace(name,std::move(table.at("t")));};
    std::vector<int64_t> keys,keep;for(int i=0;i<10;++i){keys.push_back(-100+2*i+shift);keep.push_back(i!=8);}
    add("o",test::Table({{test::Raw(keys,4),test::Raw(keep,4)}},{{"k",integer},{"keep",integer}}));
    add("s",test::Table({{test::Raw({10,11,12,duplicate?12:13},4),test::Raw({0,0,1,0},4),test::Dict<StringType>({"same","same","foreign","other"})}},
        {{"k",integer},{"n",integer},{"name",std::make_shared<StringType>()}}));
    add("n",test::Table({{test::Raw({0,1},4),test::Raw({1,0},4)}},{{"k",integer},{"keep",integer}}));return tables;
}
std::string WaitSQL(std::string extra="",std::string inner="",std::string comparison=">") {
    return "SELECT s.name,COUNT(*) AS numwait FROM s,t l1,o,n WHERE s.k=l1.s AND o.k=l1.o AND o.keep=1 "
        "AND l1.receipt>l1.commit AND EXISTS (SELECT * FROM t l2 WHERE l2.o=l1.o AND l2.s<>l1.s"+inner+
        ") AND NOT EXISTS (SELECT * FROM t l3 WHERE l3.o=l1.o AND l3.s<>l1.s AND l3.receipt"+comparison+"l3.commit) "
        "AND s.n=n.k AND n.keep=1"+extra+" GROUP BY s.name ORDER BY numwait DESC,s.name";
}
}
TEST(SupplierWait, CorrelationsDatesDuplicateLabelsAndSplitRunsMatchReference) {
    for(int shift:{0,200}) {
        auto tables=WaitTables(shift);
        for(const std::string extra:{"", " AND n.k<0", " AND s.k>10", " AND o.k<100"})
        for(const std::string slice:{"", " LIMIT 1 OFFSET 1"}) {
            SCOPED_TRACE(extra+slice);auto q=ParseSelect(WaitSQL(extra)+slice,tables);ASSERT_TRUE(q.ok())<<q.status().Message();
            auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok())<<expected.status().Message();
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteSupplierWait(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
                ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
            }
        }
    }
}
TEST(SupplierWait, UnprovedOrderingKeysOrCorrelationsRetainFallback) {
    for(int scenario=0;scenario<5;++scenario) {
        auto tables=WaitTables(0,scenario==0,scenario==1);
        auto q=ParseSelect(WaitSQL(scenario==2?" AND l1.s>10":"",scenario==3?" AND l2.s>10":"",scenario==4?">=":">"),tables);
        ASSERT_TRUE(q.ok());EXPECT_FALSE(ExecuteSupplierWait(*q,true,true).has_value());
        auto expected=ExecuteRelational(*q,false,false),actual=ExecuteQuery(*q,true,true);
        ASSERT_TRUE(expected.ok());ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
    }
}
TEST(SupplierWait, EnforcesQueryBudget) {
    auto tables=WaitTables();auto parsed=ParseSelect(WaitSQL(),tables);ASSERT_TRUE(parsed.ok());
    auto q=std::move(parsed).ValueUnsafe();q.memoryBudgetBytes=1;q.allowSpill=false;
    auto actual=ExecuteSupplierWait(q,true,true);ASSERT_TRUE(actual.has_value());EXPECT_FALSE(actual->ok());
}
