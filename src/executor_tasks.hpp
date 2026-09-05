#pragma once
#include <optional>
#include <type_traits>
#include <vector>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>
namespace pgaccel {
inline tbb::task_arena &QueryArena() {
    static tbb::task_arena arena(8);
    return arena;
}
// Blocking completion keeps query resources alive until every task is done,
// including cancellation following an exception. One task stays on the caller.
template<class F> auto RunTasks(int count,F task) {
    using Value=std::invoke_result_t<F,int>;
    std::vector<std::optional<Value>> slots(count);
    auto run=[&](int index){slots[index].emplace(task(index));};
    if(count==1)run(0);
    else QueryArena().execute([&]{tbb::parallel_for(0,count,run);});
    std::vector<Value> results;results.reserve(count);
    for(auto &slot:slots)results.push_back(std::move(*slot));
    return results;
}
}
