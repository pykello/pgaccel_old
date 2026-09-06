#include "test_table.hpp"
#include <gtest/gtest.h>
namespace pgaccel {
std::optional<Result<QueryOutput>> ExecuteConvergentSum(const QueryDesc &,bool,bool);
Result<QueryOutput> ExecuteRelational(const QueryDesc &,bool,bool);
}
using namespace pgaccel;

TEST(IdentityPropagation, VectorWidthsSignedBoundsMasksAndStripeTails) {
    for(int width:{1,2,4}) {
        int64_t first=width==1?-120:width==2?-32000:int64_t(INT32_MAX)-8;
        std::vector<std::vector<ColumnDataP>> a,b,f;
        for(int g=0;g<2;++g) {
            std::vector<int64_t> ak,bk,nations,keep;
            for(int i=0;i<259;++i) {
                int k=g*259+i;ak.push_back(2000+2*k);bk.push_back(5000+k);
                nations.push_back(first+(k%10)-1);keep.push_back(k%3);
            }
            a.push_back({test::Raw(ak),test::Raw(nations,width),test::Raw(keep)});
            std::rotate(nations.begin(),nations.begin()+3,nations.end());
            b.push_back({test::Raw(bk),test::Raw(nations,width),test::Raw(keep)});
            std::vector<int64_t> fa,fb,amount;
            for(int i=0;i<521;++i) {
                fa.push_back(2000+2*(i%540)+(i%27==0));fb.push_back(5000+(i*7)%540);amount.push_back(i-270);
            }
            f.push_back({test::Raw(fa,4),test::Raw(fb,4),test::Raw(amount,4)});
        }
        auto integer=std::make_shared<Int64Type>();
        auto tables=test::Table(std::move(f),{{"a",integer},{"b",integer},{"x",integer}});
        auto left=test::Table(std::move(a),{{"k",integer},{"n",integer},{"keep",integer}});
        auto right=test::Table(std::move(b),{{"k",integer},{"n",integer},{"keep",integer}});
        std::vector<int64_t> nk,groups;for(int i=0;i<8;++i){nk.push_back(first+i);groups.push_back(i%2);}
        auto nation=test::Table({{test::Raw(nk,width),test::Raw(groups)}},{{"k",integer},{"g",integer}});
        tables.emplace("a",std::move(left.at("t")));tables.emplace("b",std::move(right.at("t")));tables.emplace("n",std::move(nation.at("t")));
        for(const std::string filter:{"", " AND a.keep=1", " AND b.keep=1", " AND n.g=1", " AND n.g<0"}) {
            auto q=ParseSelect("SELECT n.g,SUM(t.x) FROM t,a,b,n WHERE t.a=a.k AND t.b=b.k "
                "AND a.n=b.n AND b.n=n.k"+filter+" GROUP BY n.g ORDER BY n.g",tables);
            ASSERT_TRUE(q.ok())<<q.status().Message();
            auto expected=ExecuteRelational(*q,false,false);ASSERT_TRUE(expected.ok());
            for(bool avx:{false,true})for(bool parallel:{false,true}) {
                auto actual=ExecuteConvergentSum(*q,avx,parallel);ASSERT_TRUE(actual.has_value());
                ASSERT_TRUE(actual->ok())<<actual->status().Message();EXPECT_EQ((**actual).values,expected->values);
            }
        }
    }
}
