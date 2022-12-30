#include <iostream>
#include <string>
#include <map>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <sys/types.h>
#include <pwd.h>

#include "column_data.hpp"
#include "executor.h"
#include "parser.h"
#include "types.hpp"
#include "columnar_table.h"
#include "result_type.hpp"
#include "util.h"

using namespace pgaccel;
using namespace std;

#define REQUIRED_ARGS(MIN, MAX) \
    if (args.size() < MIN || args.size() > MAX) \
    {\
        if (MIN == MAX) \
            return Status::Invalid(commandName.c_str(), " requires ", MIN, " args."); \
        if (MIN == MAX - 1) \
            return Status::Invalid(commandName.c_str(), " requires ", MIN, " or ", MAX, " args."); \
        else \
            return Status::Invalid(commandName.c_str(), " requires ", MIN, "..", MAX, " args."); \
    }

const std::string path = "/home/hadi/data/tpch/1/parquet/lineitem.parquet";
const char * HISTORY_FILE = ".pgaccel_history";

struct ReplState {
    TableRegistry tables;
    bool done = false;
    bool timingEnabled = true;
    bool useAvx = true;
};

struct ReplCommand {
    std::string name;
    Result<bool> (*func)(ReplState &state,
                         const std::string &commandName,
                         const vector<std::string> &args,
                         const std::string &commandText);
};

// static function forward declarations
static int repl();
static bool CommandTerminated(const std::string &s);
static Result<bool> ProcessCommand(ReplState &state, const std::string &commandStr);
static std::vector<std::string> TokenizeCommand(const std::string &s);
static Result<bool> ParseBool(const std::string& s);
static std::string HistoryFile();

// commands
static Result<bool> ProcessHelp(ReplState &state,
                                const std::string &commandName,
                                const vector<std::string> &args,
                                const std::string &commandText);
static Result<bool> ProcessTiming(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args,
                                  const std::string &commandText);
static Result<bool> ProcessAvx(ReplState &state,
                               const std::string &commandName,
                               const vector<std::string> &args,
                               const std::string &commandText);
static Result<bool> ProcessLoadParquet(ReplState &state, 
                                       const std::string &commandName,
                                       const vector<std::string> &args,
                                       const std::string &commandText);
static Result<bool> ProcessSelect(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args,
                                  const std::string &commandText);
static Result<bool> ProcessQuit(ReplState &state,
                                const std::string &commandName,
                                const vector<std::string> &args,
                                const std::string &commandText);
static Result<bool> ProcessSchema(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args,
                                  const std::string &commandText);

std::vector<ReplCommand> commands = {
    { "help", ProcessHelp },
    { "quit", ProcessQuit },
    { "timing", ProcessTiming },
    { "load_parquet", ProcessLoadParquet },
    { "select", ProcessSelect },
    { "schema", ProcessSchema },
    { "avx", ProcessAvx }
};

// singal handler stuff
sigjmp_buf ctrlc_buf;

void
signal_callback_handler(int signum) {
    if (signum == SIGINT) {
        std::cout << std::endl;
        siglongjmp(ctrlc_buf, 1);
    }
}

// repl definitions

static int
repl()
{
    ReplState state;

    if (signal(SIGINT, signal_callback_handler) == SIG_ERR) {
        std::cout << "failed to register interrupts with kernel" << std::endl;
        return -1;
    }

    std::string historyFile = HistoryFile();
    read_history(historyFile.c_str());

    std::string line;

    while (!state.done) {
        while ( sigsetjmp( ctrlc_buf, 1 ) != 0 );

        std::string prompt;
        if (line.length())
            prompt = "== ";
        else
            prompt = ">> ";

        char *buf = readline(prompt.c_str());
        if (buf == nullptr) {
            state.done = true;
            break;
        }

        if (line.length())
            line += "\n";
        line += buf;

        if (line.length() > 0 && CommandTerminated(line)) {
            add_history(line.c_str());
            auto result = ProcessCommand(state, line);
            if (!result.ok()) {
                std::cout << "ERROR: " << result.status().Message() << std::endl;
            }
            line = "";
        }

        // readline malloc's a new buffer every time.
        free(buf);
    }

    write_history(historyFile.c_str());

    return 0;
}

static bool
CommandTerminated(const std::string &s)
{
    int i = s.length() - 1;
    while (i >= 0 && isspace(s[i]))
        i--;
    return i >= 0 && s[i] == ';';
}

static Result<bool>
ProcessCommand(ReplState &state, const std::string &commandStr)
{
    auto tokens = TokenizeCommand(commandStr);
    if (tokens.size() == 0)
        return true;

    std::string commandName = ToLower(tokens[0]);
    auto args = std::vector<std::string>(tokens.begin() + 1, tokens.end());

    for (auto cmd: commands)
    {
        if (commandName == cmd.name) {
            return cmd.func(state, commandName, args, commandStr);
        }
    }

    return Status::Invalid("unknown command: ", commandName, ".");
}

static std::vector<std::string>
TokenizeCommand(const std::string &s)
{
    return Split(s, [](char c) { return isspace(c) || c == ';'; });
}

static Result<bool>
ParseBool(const std::string& s)
{
    std::string lc = ToLower(s);
    if (lc == "true" || lc == "on")
        return true;
    if (lc == "false" || lc == "off")
        return false;
    return Status::Invalid("Invalid boolean: ", lc);
}

static std::string
HistoryFile()
{
    const char *homedir;
    if ((homedir = getenv("HOME")) == NULL) {
        homedir = getpwuid(getuid())->pw_dir;
    }

    std::string result;
    result += homedir;
    result += "/";
    result += HISTORY_FILE;

    return result;
}

static Result<bool>
ProcessHelp(ReplState &state,
            const std::string &commandName,
            const vector<std::string> &args,
            const std::string &commandText)
{
    REQUIRED_ARGS(0, 1);
    std::cout << "Available Commands: " << std::endl;
    for (auto cmd: commands) {
        std::cout << "  - " << cmd.name << std::endl;
    }
    return true;
}

static Result<bool>
ProcessTiming(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args,
              const std::string &commandText)
{
    REQUIRED_ARGS(0, 1);
    if (args.size() == 1)
        ASSIGN_OR_RAISE(state.timingEnabled, ParseBool(args[0]));
    std::cout << "Timing is " << (state.timingEnabled ? "on." : "off.") << std::endl;
    return true;
}

static Result<bool>
ProcessAvx(ReplState &state,
           const std::string &commandName,
           const vector<std::string> &args,
           const std::string &commandText)
{
    REQUIRED_ARGS(0, 1);
    if (args.size() == 1)
        ASSIGN_OR_RAISE(state.useAvx, ParseBool(args[0]));
    std::cout << "AVX is " << (state.useAvx ? "on." : "off.") << std::endl;
    return true;
}

static Result<bool>
ProcessLoadParquet(ReplState &state,
                   const std::string &commandName,
                   const vector<std::string> &args,
                   const std::string &commandText)
{
    REQUIRED_ARGS(2, 3);

    std::string tableName = ToLower(args[0]);
    std::string path = args[1];
    std::optional<std::set<std::string>> fields;
    if (args.size() == 3) {
        auto fieldsVec = Split(args[2], [](char c) { return c == ','; });
        fields = std::set<std::string>(fieldsVec.begin(), fieldsVec.end());
    }

    std::unique_ptr<ColumnarTable> table;

    auto durationMs = MeasureDurationMs([&]() {
        table = ColumnarTable::ImportParquet(tableName, path, fields);
    });

    if (!table)
        return Status::Invalid("Failed to load a parquet file from ", path);

    if (state.timingEnabled)
        std::cout << "Duration: " << durationMs << "ms" << std::endl;
    
    state.tables[tableName] = std::move(table);

    return true;
}

static Result<bool>
ProcessSelect(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args,
              const std::string &commandText)
{
    QueryDesc queryDesc;
    ASSIGN_OR_RAISE(queryDesc, ParseSelect(commandText, state.tables));
    std::cout << queryDesc.ToString() << std::endl;

    Result<QueryOutput> queryOutput(Status::Invalid(""));

    auto durationMs = MeasureDurationMs([&]() {
       queryOutput = ExecuteQuery(queryDesc, state.useAvx);
    });

    if (!queryOutput.ok())
        return queryOutput.status();

    std::cout << queryOutput->values[0][0] << std::endl;

    if (state.timingEnabled)
        std::cout << "Duration: " << durationMs << "ms" << std::endl;

    return true;
}

static Result<bool>
ProcessQuit(ReplState &state,
            const std::string &commandName,
            const vector<std::string> &args,
            const std::string &commandText)
{
    REQUIRED_ARGS(0, 0);
    state.done = true;
    return true;
}

static Result<bool>
ProcessSchema(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args,
              const std::string &commandText)
{
    REQUIRED_ARGS(1, 1);
    std::string tableName = ToLower(args[0]);

    if (state.tables.count(tableName) == 0)
        return Status::Invalid("Table not found: ", tableName);

    const auto &schema = state.tables[tableName]->Schema();
    
    for (const auto &field: schema)
    {
        std::cout << "  " << field.name << ": " << field.type->ToString() << std::endl;
    }

    return true;
}

void FileDebugInfo(parquet::FileMetaData &fileMetadata)
{
    auto schema = fileMetadata.schema();
    cout << "Row groups: " << fileMetadata.num_row_groups() << endl;
    cout << "Columns: " << fileMetadata.num_columns() << endl;
    for (int col = 0; col < schema->num_columns(); col++) {
        auto column = schema->Column(col);
        cout << "  " << column->name() << "," << parquet::TypeToString(column->physical_type()) << endl;
    }
}

void MeasurePerf(const std::function<void()> &body)
{
    auto durationMs = MeasureDurationMs(body);

    std::cout << "Duration: " << durationMs << "ms" << std::endl;
}

int main() {
    return repl();
    // std::vector<std::pair<std::string, std::string>> queries = {
    //     {"L_SHIPMODE", "AIR"},          // 858104
    //     {"L_SHIPDATE", "1996-02-12"},   // 2441
    //     {"L_QUANTITY", "1"},            // 120401
    //     {"L_ORDERKEY", "1"},            // 6
    // };

    // std::set<std::string> fieldsToLoad;
    // for (auto q: queries)
    //     fieldsToLoad.insert(q.first);
    
    // auto columnarTable = pgaccel::ColumnarTable::ImportParquet(path, fieldsToLoad);
    // if (!columnarTable.has_value()) {
    //     std::cout << "Failed to load parquet file" << std::endl;
    //     exit(-1);
    // }

    // for (auto q: queries) {
    //     std::string columnName = q.first;
    //     std::string value = q.second;

    //     auto maybeColumnIdx = columnarTable->ColumnIndex(columnName);
    //     if (!maybeColumnIdx.has_value()) {
    //         std::cout << "Column not found" << std::endl;
    //         exit(-1);
    //     }

    //     int columnIdx = *maybeColumnIdx;
    //     const auto &columnDataVec = columnarTable->ColumnData(columnIdx);
    //     const auto &columnDesc = columnarTable->Schema()[columnIdx];

    //     MeasurePerf([&]() {
    //         int total = CountMatches(columnDataVec, value, columnDesc.type.get(), false);
    //         cout << "matches (no avx): " << total << endl;
    //     });

    //     MeasurePerf([&]() {
    //         int total = CountMatches(columnDataVec, value, columnDesc.type.get(), true);
    //         cout << "matches (avx): " << total << endl;
    //     });
    // }

    return 0;
}
