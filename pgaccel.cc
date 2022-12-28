#include <iostream>
#include <string>
#include <map>
#include <chrono>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "column_data.hpp"
#include "executor.h"
#include "types.hpp"
#include "columnar_table.h"
#include "util.h"

using namespace pgaccel;
using namespace std;
using namespace std::chrono;

using arrow::Result;
using arrow::Status;

#define ASSIGN_OR_RAISE(var, result) \
    do {\
        if (result.ok()) \
            var = std::move(*result); \
        else \
            return result; \
    } while(0);

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

struct ReplState {
    std::map<std::string, pgaccel::ColumnarTable> tables;
    bool done = false;
    bool timingEnabled = true;
    bool useAvx = true;
};

struct ReplCommand {
    std::string name;
    Result<bool> (*func)(ReplState &state,
                         const std::string &commandName,
                         const vector<std::string> &args);
};

// static function forward declarations
static int repl();
static bool CommandTerminated(const std::string &s);
static Result<bool> ProcessCommand(ReplState &state, const std::string &commandStr);
static std::vector<std::string> TokenizeCommand(const std::string &s);
static Result<bool> ParseBool(const std::string& s);

// commands
static Result<bool> ProcessHelp(ReplState &state,
                                const std::string &commandName,
                                const vector<std::string> &args);
static Result<bool> ProcessTiming(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args);
static Result<bool> ProcessLoadParquet(ReplState &state, 
                                       const std::string &commandName,
                                       const vector<std::string> &args);
static Result<bool> ProcessSelect(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args);
static Result<bool> ProcessQuit(ReplState &state,
                                const std::string &commandName,
                                const vector<std::string> &args);
static Result<bool> ProcessSchema(ReplState &state,
                                  const std::string &commandName,
                                  const vector<std::string> &args);

std::vector<ReplCommand> commands = {
    { "help", ProcessHelp },
    { "quit", ProcessQuit },
    { "timing", ProcessTiming },
    { "load_parquet", ProcessLoadParquet },
    { "select", ProcessSelect },
    { "schema", ProcessSchema }
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
                std::cout << "ERROR: " << result.status().message() << std::endl;
            }
            line = "";
        }

        // readline malloc's a new buffer every time.
        free(buf);
    }

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
            return cmd.func(state, commandName, args);
        }
    }

    return Status::Invalid("unknown command: ", commandName, ".");
}

static std::vector<std::string>
TokenizeCommand(const std::string &s)
{
    std::vector<std::string> tokens;
    std::string current;
    for (int i = 0; i < s.length(); i++) {
        if (isspace(s[i]) || s[i] == ';') {
            if (current.length())
                tokens.push_back(current);
            current = "";
        } else {
            current += s[i];
        }
    }
    return tokens;
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

static Result<bool>
ProcessHelp(ReplState &state,
            const std::string &commandName,
            const vector<std::string> &args)
{
    // todo
    REQUIRED_ARGS(0, 0);
    return true;
}

static Result<bool>
ProcessTiming(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args)
{
    // todo
    REQUIRED_ARGS(0, 1);
    if (args.size() == 1)
        ASSIGN_OR_RAISE(state.timingEnabled, ParseBool(args[0]));
    std::cout << "Timing is " << (state.timingEnabled ? "on." : "off.") << std::endl;
    return true;
}

static Result<bool>
ProcessLoadParquet(ReplState &state,
                   const std::string &commandName,
                   const vector<std::string> &args)
{
    // todo
    REQUIRED_ARGS(2, 3);
    return true;
}

static Result<bool>
ProcessSelect(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args)
{
    // todo
    return true;
}

static Result<bool>
ProcessQuit(ReplState &state,
            const std::string &commandName,
            const vector<std::string> &args)
{
    // todo
    REQUIRED_ARGS(0, 0);
    state.done = true;
    return true;
}

static Result<bool>
ProcessSchema(ReplState &state,
              const std::string &commandName,
              const vector<std::string> &args)
{
    // todo
    REQUIRED_ARGS(1, 1);
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
    auto start = high_resolution_clock::now();

    body();

    auto stop = high_resolution_clock::now();
    auto duration  = duration_cast<microseconds>(stop - start);

    std::cout << "Duration: " << duration.count() / 1000 << "ms" << std::endl;
}

int main() {
    return repl();
    std::vector<std::pair<std::string, std::string>> queries = {
        {"L_SHIPMODE", "AIR"},          // 858104
        {"L_SHIPDATE", "1996-02-12"},   // 2441
        {"L_QUANTITY", "1"},            // 120401
        {"L_ORDERKEY", "1"},            // 6
    };

    std::set<std::string> fieldsToLoad;
    for (auto q: queries)
        fieldsToLoad.insert(q.first);
    
    auto columnarTable = pgaccel::ColumnarTable::ImportParquet(path, fieldsToLoad);
    if (!columnarTable.has_value()) {
        std::cout << "Failed to load parquet file" << std::endl;
        exit(-1);
    }

    for (auto q: queries) {
        std::string columnName = q.first;
        std::string value = q.second;

        auto maybeColumnIdx = columnarTable->ColumnIndex(columnName);
        if (!maybeColumnIdx.has_value()) {
            std::cout << "Column not found" << std::endl;
            exit(-1);
        }

        int columnIdx = *maybeColumnIdx;
        const auto &columnDataVec = columnarTable->ColumnData(columnIdx);
        const auto &columnDesc = columnarTable->Schema()[columnIdx];

        MeasurePerf([&]() {
            int total = CountMatches(columnDataVec, value, columnDesc.type.get(), false);
            cout << "matches (no avx): " << total << endl;
        });

        MeasurePerf([&]() {
            int total = CountMatches(columnDataVec, value, columnDesc.type.get(), true);
            cout << "matches (avx): " << total << endl;
        });
    }

    return 0;
}
