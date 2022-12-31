#include "columnar_table.h"
#include "parser.h"
#include "executor.h"

#include <gtest/gtest.h>
#include <iostream>
#include <string>

using namespace std;
using namespace pgaccel;

static void VerifyQuery(const TableRegistry &registry,
                        const string &query,
                        const vector<string> &expectedResult);

static std::string TestsDir()
{
    string file_path = __FILE__;
    string dir_path = file_path.substr(0, file_path.rfind("/"));
    return dir_path;
}

const std::string LINEITEM_PARQUET = TestsDir() + "/data/lineitem.parquet";

TEST(PgAccelTest, BasicQueries) {
    set<string> fields = { "L_ORDERKEY", "L_SHIPMODE", "L_SHIPDATE", "L_QUANTITY" };

    ColumnarTableP lineitem =
        ColumnarTable::ImportParquet("lineitem", LINEITEM_PARQUET, fields);
    ASSERT_NE(lineitem.get(), nullptr);

    TableRegistry registry;
    registry.insert ({ "lineitem", std::move(lineitem) });

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_ORDERKEY=1;",
                { "6" });

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR';",
                { "28551" });

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPDATE='1996-02-12';",
                { "94" });

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_QUANTITY=2;",
                { "4004" });
}


static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<string> &expectedResult,
            bool useAvx)
{
    auto parsed = ParseSelect(query, registry);
    if (!parsed.ok())
        cout << parsed.status().Message() << endl;
    ASSERT_TRUE(parsed.ok());

    auto result = ExecuteQuery(*parsed, useAvx);
    ASSERT_TRUE(result.ok());

    ASSERT_EQ(result->values.size(), 1);
    ASSERT_EQ(result->values[0], expectedResult);
}

static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<string> &expectedResult)
{
    VerifyQuery(registry, query, expectedResult, true);
    VerifyQuery(registry, query, expectedResult, false);
}
