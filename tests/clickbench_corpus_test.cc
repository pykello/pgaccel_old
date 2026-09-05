#include "test_table.hpp"
#include <fstream>
#include <filesystem>
#include <gtest/gtest.h>
using namespace pgaccel;
TEST(ClickBench, EntirePinnedCorpusBindsAndExecutes) {
    auto root=std::filesystem::path(__FILE__).parent_path().parent_path();
    std::ifstream schemaFile(root/"benchmarks/clickbench/hits.schema");
    ASSERT_TRUE(schemaFile.good());
    std::vector<ColumnDataP> columns;
    std::vector<std::pair<std::string,std::shared_ptr<AccelType>>> schema;
    std::string name,type;
    while(schemaFile>>name>>type) {
        std::shared_ptr<AccelType> logical;
        if(type=="TEXT") {logical=std::make_shared<StringType>();columns.push_back(test::Dict<StringType>({""}));}
        else if(type=="Date") {logical=std::make_shared<DateType>();columns.push_back(test::Dict<DateType>({0}));}
        else {
            logical=type=="TIMESTAMP"?std::shared_ptr<AccelType>(std::make_shared<TimestampType>()):
                    type=="BIGINT"?std::shared_ptr<AccelType>(std::make_shared<Int64Type>()):
                                   std::shared_ptr<AccelType>(std::make_shared<Int32Type>());
            columns.push_back(test::Raw({0},type=="BIGINT" || type=="TIMESTAMP"?8:4));
        }
        schema.push_back({name,logical});
    }
    ASSERT_EQ(schema.size(),105);
    auto tables=test::Table({columns},schema);auto table=tables.extract("t");table.key()="hits";tables.insert(std::move(table));
    std::ifstream queries(root/"benchmarks/clickbench/queries.sql");
    ASSERT_TRUE(queries.good());std::string sql;int id=0;
    while(std::getline(queries,sql)) {
        ++id;auto query=ParseSelect(sql,tables);ASSERT_TRUE(query.ok())<<id<<": "<<query.status().Message();
        auto output=ExecuteQuery(*query,true,true);ASSERT_TRUE(output.ok())<<id<<": "<<output.status().Message();
    }
    EXPECT_EQ(id,43);
}
