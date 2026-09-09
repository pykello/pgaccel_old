#include "columnar_table.h"
#include "executor.h"
#include "direct_query.hpp"
#include "native_block_cache.hpp"
#include "query_resources.hpp"
#include "util.h"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace py = pybind11;
using namespace pgaccel;
using Clock = std::chrono::steady_clock;

class Database {
    TableRegistry tables;
    std::unique_ptr<NativeBlockCache> inputCache;
    std::mutex mutex;
    uint64_t memoryBytes, spillBytes;
    bool parallel, avx, direct, closed = false;
    std::string directDirectory, residentSQL;
    double loadMs = 0;
public:
    Database(const std::string &directory, uint64_t memory, uint64_t spill,
             bool parallel, bool avx, bool direct, uint64_t cacheBytes)
        : memoryBytes(memory), spillBytes(spill), parallel(parallel), avx(avx),
          direct(direct), directDirectory(directory) {
        if(direct && cacheBytes)inputCache=std::make_unique<NativeBlockCache>(cacheBytes);
        py::gil_scoped_release release;
        auto start = Clock::now();
        std::vector<std::filesystem::path> files;
        for (const auto &entry : std::filesystem::directory_iterator(directory))
            if (entry.is_regular_file() && entry.path().extension() == ".pgaccel")
                files.push_back(entry.path());
        std::sort(files.begin(), files.end());
        if (files.empty()) throw std::invalid_argument("No native tables found");
        for (const auto &path : files) {
            auto name = ToLower(path.stem().string());
            auto loaded = direct ? ColumnarTable::LoadSchema(name, path.string())
                                 : ColumnarTable::Load(name, path.string());
            if (!loaded.ok()) throw std::runtime_error(loaded.status().Message());
            if (!tables.emplace(name, std::move(loaded).ValueUnsafe()).second)
                throw std::invalid_argument("Duplicate table name: " + name);
        }
        if (direct) tables.clear();
        loadMs = std::chrono::duration<double, std::milli>(Clock::now()-start).count();
    }

    py::dict execute(const std::string &sql) {
        QueryOutput output;
        double ms, prepareMs = 0;
        uint64_t directBytes = 0, cacheHits = 0, cacheMisses = 0, cacheBytes = 0;
        {
            // Drop the lock before reacquiring the GIL: another Python thread
            // may be waiting for the same database while holding the GIL.
            py::gil_scoped_release release;
            std::lock_guard lock(mutex);
            if (closed) throw std::runtime_error("Database is closed");
            if (direct && (tables.empty() || sql != residentSQL)) {
                auto start = Clock::now();
                // Release the previous working set before allocating the next.
                // Failed preparation must not leave a reusable SQL marker.
                residentSQL.clear();
                tables.clear();
#if defined(__GLIBC__)
                // Stripe allocations can leave free pages in malloc arenas.
                // Return them before retaining the next statement's columns;
                // include release cost in preparation, never in warm reuse.
                malloc_trim(0);
#endif
                auto prepared = PrepareDirectQuery(directDirectory, sql, avx, parallel, inputCache.get());
                if (!prepared.ok())
                    throw std::invalid_argument(prepared.status().Message());
                auto owned = std::move(prepared).ValueUnsafe();
                directBytes = owned.loadIO.bytesRead;
                cacheHits = owned.loadIO.cacheHits; cacheMisses = owned.loadIO.cacheMisses;
                tables = std::move(owned.tables);
                residentSQL = sql;
                prepareMs = std::chrono::duration<double, std::milli>(
                    Clock::now()-start).count();
            }
            cacheBytes = inputCache ? inputCache->Bytes() : 0;
            auto parsed = ParseSelect(sql, tables);
            if (!parsed.ok()) throw std::invalid_argument(parsed.status().Message());
            auto query = std::move(parsed).ValueUnsafe();
            SetQueryResourceLimits(query, memoryBytes, spillBytes);
            auto start = Clock::now();
            auto result = ExecuteQuery(query, avx, parallel);
            ms = std::chrono::duration<double, std::milli>(Clock::now()-start).count();
            if (!result.ok()) throw std::runtime_error(result.status().Message());
            output = std::move(result).ValueUnsafe();
        }
        // Reuse immutable strings only within this result conversion. Keys
        // reference output, which remains alive until conversion is complete.
        std::unordered_map<std::string_view, py::str> strings;
        py::list rows(output.values.size());
        for (size_t i = 0; i < output.values.size(); ++i) {
            const auto &source = output.values[i];
            py::list row(source.size());
            for (size_t j = 0; j < source.size(); ++j) {
                auto found = strings.find(source[j]);
                if (found != strings.end()) row[j] = found->second;
                else {
                    py::str value(source[j]);
                    row[j] = value;
                    if (strings.size() < 4096) strings.emplace(source[j], value);
                }
            }
            rows[i] = std::move(row);
        }
        py::dict result;
        result["rows"] = std::move(rows);
        result["columns"] = py::cast(output.fieldNames);
        result["execution_ms"] = ms;
        result["prepare_ms"] = prepareMs;
        result["cache_hits"] = cacheHits; result["cache_misses"] = cacheMisses;
        result["cache_bytes"] = cacheBytes;
        result["direct_load_bytes"] = directBytes;
        result["peak_memory_bytes"] = output.peakMemoryBytes;
        result["spill_bytes"] = output.spillBytes;
        return result;
    }

    void close() {
        py::gil_scoped_release release;
        std::lock_guard lock(mutex);
        closed = true;
        if(inputCache)inputCache->Clear();
        tables.clear();
    }
    double load_ms() const { return loadMs; }
};

PYBIND11_MODULE(pgaccel_native, module) {
    module.doc() = "Read-only native pgaccel tables; result cells are strings.";
    py::class_<Database>(module, "Database")
        .def(py::init<const std::string &, uint64_t, uint64_t, bool, bool, bool, uint64_t>(),
             py::arg("directory"), py::arg("memory_bytes") = uint64_t(64)<<30,
             py::arg("spill_bytes") = uint64_t(512)<<30,
             py::arg("parallel") = true, py::arg("avx") = true,
             py::arg("direct") = false, py::arg("cache_bytes") = 0)
        .def("execute", &Database::execute, py::arg("sql"))
        .def("close", &Database::close)
        .def_property_readonly("load_ms", &Database::load_ms)
        .def("__enter__", [](Database &db) -> Database & { return db; },
             py::return_value_policy::reference_internal)
        .def("__exit__", [](Database &db, py::object, py::object, py::object) {
            db.close();
        });
}
