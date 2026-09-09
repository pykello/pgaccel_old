#include "direct_query.hpp"
#include "test_table.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
using namespace pgaccel;
TEST(PredicateColumns, CompressOnlyUnobservedLabelsAcrossScopes) {
    char directory[]="/tmp/pgaccel-predicate-columns-XXXXXX";ASSERT_NE(mkdtemp(directory),nullptr);
    struct Cleanup{const char *p;~Cleanup(){std::filesystem::remove_all(p);}}cleanup{directory};
    auto tables=test::Table({{test::Raw({1,1,2,2}),test::Dict<StringType>({"keep a","skip a","keep b","keep c"})}},
        {{"k",std::make_shared<Int64Type>()},{"s",std::make_shared<StringType>()}});
    auto c=test::Table({{test::Raw({1,2,3})}},{{"k",std::make_shared<Int64Type>()}});
    tables.emplace("c",std::move(c.at("t")));
    for(const auto &[name,table]:tables)ASSERT_TRUE(table->Save(std::string(directory)+"/"+name+".pgaccel").ok());
    for(const auto &sql:std::vector<std::string>{
        "SELECT c.k,count(t.k) FROM c LEFT JOIN t ON c.k=t.k AND t.s NOT LIKE '%skip%' GROUP BY c.k ORDER BY c.k",
        "SELECT count(*) FROM t WHERE s LIKE '%keep%'",
        "SELECT s,count(*) FROM t WHERE s LIKE '%keep%' GROUP BY s ORDER BY s",
        "SELECT count(*) FROM t WHERE s LIKE '%keep%' OR s LIKE '% a'",
        "SELECT count(*) FROM t WHERE s LIKE '%keep%' AND k IN (SELECT k FROM t WHERE s LIKE '% a')"}) {
        SCOPED_TRACE(sql);auto parsed=ParseSelect(sql,tables);ASSERT_TRUE(parsed.ok())<<parsed.status().Message();
        auto expected=ExecuteQuery(*parsed,true,true);ASSERT_TRUE(expected.ok());
        auto prepared=PrepareDirectQuery(directory,sql);ASSERT_TRUE(prepared.ok())<<prepared.status().Message();
        auto actual=ExecuteQuery(prepared->query,true,true);ASSERT_TRUE(actual.ok());EXPECT_EQ(actual->values,expected->values);
        auto allowed=PredicateOnlyColumns(*parsed);
        if(sql.find("LEFT JOIN")!=std::string::npos) {
            ASSERT_TRUE(allowed[tables.at("t").get()].count("s"));
            const auto &table=*prepared->tables.at("t");auto index=*table.ColumnIndex("s");
            EXPECT_EQ(table.GetRowGroup(0).size,4);
            EXPECT_EQ(static_cast<const DictColumnDataBase*>(table.GetRowGroup(0).columns[index].get())->dictSize(),2);
        }
        if(sql.find("GROUP BY s")!=std::string::npos || sql.find("% a")!=std::string::npos)
            EXPECT_FALSE(allowed[tables.at("t").get()].count("s"));
    }
}
TEST(PredicateColumns, RepresentativeCodesPreserveEveryPredicateBit) {
    std::vector<std::string> labels;
    for(int i=0;i<300;++i)labels.push_back(std::to_string(1000+i));
    auto input=test::Dict<StringType>(labels);
    auto tables=test::Table({{input}},{{"s",std::make_shared<StringType>()}});
    for(auto sql:{"SELECT count(*) FROM t WHERE s < '1100'","SELECT count(*) FROM t WHERE s LIKE '%2%'",
                  "SELECT count(*) FROM t WHERE s NOT IN ('1001','1299')"}) {
        auto q=ParseSelect(sql,tables);ASSERT_TRUE(q.ok());ASSERT_EQ(q->filterClauses.size(),1);
        auto f=q->filterClauses[0];auto compact=PredicateDictionary(input,f,true);ASSERT_TRUE(compact.ok());
        RowGroup before;before.columns={input};before.size=300;
        RowGroup after;after.columns={*compact};after.size=300;
        auto node=CreateFilterNode({f},true);uint8_t a[RowGroupSize/8]{},b[RowGroupSize/8]{};
        EXPECT_EQ(node->ExecuteSet(before,a),node->ExecuteSet(after,b));
        EXPECT_EQ(memcmp(a,b,(300+7)/8),0);
        auto dictionary=static_cast<const DictColumnDataBase*>((*compact).get());EXPECT_LE(dictionary->dictSize(),2);EXPECT_EQ(dictionary->bytesPerValue(),1);
        EXPECT_EQ(static_cast<const DictColumnDataBase*>(input.get())->dictSize(),300);
    }
}

TEST(PredicateColumns, ParallelLabelsPreserveBitsAtWordAndTaskBoundaries) {
    for(int count:{0,63,64,65,8191,8192,8193,65536}) {
        std::vector<std::string> labels;
        for(int i=0;i<count;++i) {
            std::string suffix=i%3==0 ? "special requests" : i%3==1 ? "special_é requests" : "ordinary";
            labels.push_back(std::to_string(100000+i)+" "+suffix);
        }
        auto input=test::Dict<StringType>(labels);
        auto tables=test::Table({{input}},{{"s",std::make_shared<StringType>()}});
        for(auto sql:{"SELECT count(*) FROM t WHERE s LIKE '%special%requests%'",
                      "SELECT count(*) FROM t WHERE s NOT LIKE '%special%requests%'",
                      "SELECT count(*) FROM t WHERE s LIKE '%é%'",
                      "SELECT count(*) FROM t WHERE s NOT IN ('100000 special requests')"}) {
            auto parsed=ParseSelect(sql,tables);ASSERT_TRUE(parsed.ok());ASSERT_EQ(parsed->filterClauses.size(),1);
            auto filter=parsed->filterClauses[0];
            RowGroup before;before.columns={input};before.size=count;
            for(bool avx:{false,true}) {
                auto node=CreateFilterNode({filter},avx);
                uint8_t expected[RowGroupSize/8]{};int selected=node->ExecuteSet(before,expected);
                auto compact=PredicateDictionary(input,filter,avx,true);ASSERT_TRUE(compact.ok());
                RowGroup after;after.columns={*compact};after.size=count;
                uint8_t actual[RowGroupSize/8]{};
                EXPECT_EQ(node->ExecuteSet(after,actual),selected);
                EXPECT_EQ(std::memcmp(expected,actual,(count+7)/8),0) << count << ':' << sql;
            }
        }
    }
}
