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
                        const vector<vector<string>> &expectedResult);
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
                {{ "200000" }});

    // unfiltered sum
    VerifyQuery(registry,
                "SELECT sum(l_quantity) FROM lineitem;",
                {{ "5103301.00" }});

    // filter on one column
    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_ORDERKEY=1;",
                {{ "6" }});

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR';",
                {{ "28551" }});

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPDATE='1996-02-12';",
                {{ "94" }});

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_QUANTITY=2;",
                {{ "4004" }});

    // filter on two columns
    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_QUANTITY=3 "
                "AND L_SHIPDATE='1996-02-11';",
                {{ "1" }});

    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR' "
                "AND L_SHIPDATE='1996-02-11';",
                {{ "14" }});
    
    VerifyQuery(registry,
                "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR' "
                "AND L_SHIPDATE='1996-02-11' and L_QUANTITY=10;",
                {{ "2" }});

    // non-eq filters
    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='AIR' "
            "AND L_SHIPDATE>'1996-02-11' and L_QUANTITY<=10;",
            {{ "2404" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE L_SHIPMODE='SHIP' "
            "AND L_SHIPDATE>='1996-02-11' and L_QUANTITY<10;",
            {{ "2047" }});

    // != operator
    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPMODE != 'AIR' and L_QUANTITY != 3;",
            {{ "168109" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE L_ORDERKEY != 6;",
            {{ "199999" }});

    // not found in dict cases
    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPMODE='AIR' and L_SHIPDATE > '1980-01-01';",
            {{ "28551" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPDATE > '1980-01-01' and L_SHIPMODE='AIR';",
            {{ "28551" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPDATE > '1980-01-01';",
            {{ "200000" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPMODE != 'xyz' AND L_SHIPDATE > '1996-02-01';",
            {{ "80915" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPMODE='AIR' and L_SHIPDATE < '2022-01-01';",
            {{ "28551" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPDATE < '2022-01-01' and L_SHIPMODE='AIR';",
            {{ "28551" }});

    VerifyQuery(registry,
            "SELECT count(*) FROM lineitem WHERE "
            "L_SHIPDATE < '2022-01-01';",
            {{ "200000" }});

    // group by
    VerifyQuery(registry,
            "SELECT L_SHIPMODE, count(*) FROM LINEITEM GROUP BY L_SHIPMODE;",
            { { "AIR", "28551" },
              { "FOB", "28528" },
              { "MAIL", "28657" },
              { "RAIL", "28518" },
              { "REG AIR", "28422" },
              { "SHIP", "28656" },
              { "TRUCK", "28668" }});

    VerifyQuery(registry,
            "SELECT L_SHIPMODE, count(*) FROM LINEITEM "
            "WHERE L_SHIPMODE > 'REG AIR' AND L_QUANTITY > 5 "
            "GROUP BY L_SHIPMODE;",
            { { "SHIP", "25810" },
              { "TRUCK", "25851" }});
}

static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<vector<string>> &expectedResult,
            bool useAvx,
            bool useParallel)
{
    auto parsed = ParseSelect(query, registry);
    if (!parsed.ok())
        cout << parsed.status().Message() << endl;
    ASSERT_TRUE(parsed.ok());

    auto result = ExecuteQuery(*parsed, useAvx, useParallel);
    ASSERT_TRUE(result.ok());

    ASSERT_EQ(result->values, expectedResult);
}

static void
VerifyQuery(const TableRegistry &registry,
            const string &query,
            const vector<vector<string>> &expectedResult)
{
    VerifyQuery(registry, query, expectedResult, true, true);
    VerifyQuery(registry, query, expectedResult, false, true);
}
