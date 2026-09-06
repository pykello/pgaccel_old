#pragma once
#include "test_table.hpp"

namespace pgaccel::test {
inline TableRegistry ConvergentTables(bool duplicate=false) {
    std::vector<int64_t> orders,suppliers,amounts;
    for(int i=0;i<259;++i) {
        orders.push_back(i%11<8?100+2*(i%11):999);
        suppliers.push_back(20+i%7);amounts.push_back(i-130);
    }
    auto registry=Table({{Raw(orders,4),Raw(suppliers,4),Raw(amounts,4)},
                         {Raw(orders,4),Raw(suppliers,4),Raw(amounts,4)}},
        {{"o",std::make_shared<Int64Type>()},{"s",std::make_shared<Int64Type>()},
         {"x",std::make_shared<Int64Type>()}});
    auto add=[&](std::string name,TableRegistry table) {registry.emplace(name,std::move(table.at("t")));};
    add("o",Table({{Raw({108,110,112,114}),Raw({14,15,10,10}),Raw({0,1,0,1})},
                    {Raw({100,102,104,106}),Raw({10,11,12,13}),Raw({0,1,0,1})}},
        {{"k",std::make_shared<Int64Type>()},{"c",std::make_shared<Int64Type>()},
         {"keep",std::make_shared<Int64Type>()}}));
    add("c",Table({{Raw({10,11,12,13,14,duplicate?14:15}),Raw({-1,0,1,-1,0,1})}},
        {{"k",std::make_shared<Int64Type>()},{"n",std::make_shared<Int64Type>()}}));
    add("s",Table({{Raw({20,21,22,23,24,25}),Raw({-1,0,1,-1,0,1})}},
        {{"k",std::make_shared<Int64Type>()},{"n",std::make_shared<Int64Type>()}}));
    add("n",Table({{Raw({-1,0,1}),Dict<StringType>({"same","same","other"}),Raw({7,7,8})}},
        {{"k",std::make_shared<Int64Type>()},{"label",std::make_shared<StringType>()},
         {"r",std::make_shared<Int64Type>()}}));
    add("r",Table({{Raw({7,8}),Raw({1,0})}},
        {{"k",std::make_shared<Int64Type>()},{"keep",std::make_shared<Int64Type>()}}));
    return registry;
}
inline std::string ConvergentSQL(std::string extra="") {
    return "SELECT n.label,SUM(t.x*(1-0.05)) AS amount FROM t,o,c,s,n,r "
        "WHERE t.o=o.k AND o.c=c.k AND t.s=s.k AND c.n=s.n AND s.n=n.k "
        "AND n.r=r.k AND r.keep=1"+extra+" GROUP BY n.label ORDER BY amount DESC";
}
}
