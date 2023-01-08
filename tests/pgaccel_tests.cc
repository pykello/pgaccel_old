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
static void VerifyLineitemBasic(const TableRegistry &registry);

static std::string TestsDir()
{
    string file_path = __FILE__;
    string dir_path = file_path.substr(0, file_path.rfind("/"));
    return dir_path;
}

const std::string LINEITEM_PARQUET = TestsDir() + "/data/lineitem.parquet";


class PgAccelTest : public ::testing::Test {
protected:
    void SetUp() override {
        set<string> fields = { "L_ORDERKEY", "L_SHIPMODE", "L_SHIPDATE", "L_QUANTITY" };

        ColumnarTableP lineitem =
            ColumnarTable::ImportParquet("lineitem", LINEITEM_PARQUET, fields);
        ASSERT_NE(lineitem.get(), nullptr);

        registry_parquet.insert ({ "lineitem", std::move(lineitem) });
    }

    TableRegistry registry_parquet;

};

TEST_F(PgAccelTest, BasicQueriesParquet) {
    VerifyLineitemBasic(registry_parquet);
}

TEST_F(PgAccelTest, SaveAndLoad) {
    stringstream dataStream, metadataStream;
    registry_parquet["lineitem"]->Save(metadataStream, dataStream);
    dataStream.seekg(0);
    metadataStream.seekg(0);

    Result<ColumnarTableP> lineitem =
        ColumnarTable::Load("lineitem", metadataStream, dataStream);
    ASSERT_TRUE(lineitem.ok());

    TableRegistry registry_pgaccel;
    registry_pgaccel.insert ({ "lineitem", std::move(lineitem).ValueUnsafe() });

    VerifyLineitemBasic(registry_pgaccel);
}

static void
VerifyLineitemBasic(const TableRegistry &registry)
{
    // total count
    VerifyQuery(registry,
                "SELECT count(*) from lineitem;",
                { "200000" });

    // unfiltered sum
    VerifyQuery(registry,
                "SELECT sum(l_quantity) FROM lineitem;",
                { "5103301.00" });

    // filter on one column
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

    // filter on two columns
    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_QUANTITY=3 "
                "AND L_SHIPDATE='1996-02-11';",
                { "1" });

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR' "
                "AND L_SHIPDATE='1996-02-11';",
                { "14" });
    
    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR' "
                "AND L_SHIPDATE='1996-02-11' and L_QUANTITY=10;",
                { "2" });
}

static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<string> &expectedResult,
            bool useAvx,
            bool useParallel)
{
    auto parsed = ParseSelect(query, registry);
    if (!parsed.ok())
        cout << parsed.status().Message() << endl;
    ASSERT_TRUE(parsed.ok());

    auto result = ExecuteQuery(*parsed, useAvx, useParallel);
    ASSERT_TRUE(result.ok());

    ASSERT_EQ(result->values.size(), 1);
    ASSERT_EQ(result->values[0], expectedResult);
}

static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<string> &expectedResult)
{
    VerifyQuery(registry, query, expectedResult, true, true);
    VerifyQuery(registry, query, expectedResult, false, true);
}
