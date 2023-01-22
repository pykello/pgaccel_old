#include "touca/touca.hpp"
#include "columnar_table.h"
#include "parser.h"
#include "executor.h"

#include <iostream>
#include <string>
#include <fstream>

using namespace std;
using namespace pgaccel;

struct Testcase {
    int repeat;
    string query;
};

static std::string QueriesDir()
{
    string file_path = __FILE__;
    string dir_path = file_path.substr(0, file_path.rfind("/"));
    return dir_path + "/queries";
}

static std::string TestdataDir()
{
    return getenv("PGACCEL_TOUCA_DATA_DIR");
}

static Testcase ReadTestcase(string name)
{
    Testcase result;
    ifstream fin(QueriesDir() + "/" + name);
    string line;
    fin >> result.repeat;
    while (getline(fin, line))
        result.query += "\n" + line;
    return result;
}

int main(int argc, char* argv[]) {

    set<string> fields = { "L_ORDERKEY", "L_SHIPMODE", "L_SHIPDATE",
                           "L_QUANTITY", "L_DISCOUNT" };
    string path = TestdataDir() + "/lineitem.pgaccel";
    Result<ColumnarTableP> lineitem = ColumnarTable::Load("lineitem", path, fields);
    
    if (!lineitem.ok()) {
        cout << "Failed to load the lineitem table from " << path << endl;
    }

    TableRegistry registry;
    registry.insert ({ "lineitem", std::move(lineitem).ValueUnsafe() });

    touca::workflow("queries", [&](const std::string& testcaseName) {
        auto testcase = ReadTestcase(testcaseName);

        auto parsed = ParseSelect(testcase.query, registry);
        touca::check("parsed", parsed.ok());
        if (!parsed.ok()) {
            touca::check("parse_error", parsed.status().Message());
            cout << parsed.status().Message() << endl;
            return;
        }

        touca::start_timer("execution_timer");

        Result<QueryOutput> result = Status::Invalid("");
        for (int i = 0; i < testcase.repeat; i++)
            result = ExecuteQuery(*parsed, true, true);

        touca::check("executed", result.ok());
        if (!result.ok()) {
            touca::check("exec_error", result.status().Message());
            cout << result.status().Message() << endl;
            return;
        }

        touca::stop_timer("execution_timer");
        touca::check("output", result->values);
    });

    return touca::run(argc, argv);
}
