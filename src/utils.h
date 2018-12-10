#pragma once

#include <chrono>
#include <cstdint>
#include <random>
#include <ctime>
#include <map>
#include <cstdio>
#include <string>
#include <functional>
#include <vector>

#define debug printf

#include "Nuke/thread_pool.h"
#include "Nuke/lang_extend.h"

inline uint64_t get_current_ms() {
    using namespace std::chrono;
    time_point<system_clock, milliseconds> timepoint_now = time_point_cast<milliseconds>(system_clock::now());;
    auto tmp = duration_cast<milliseconds>(timepoint_now.time_since_epoch());
    std::time_t timestamp = tmp.count();
    return (uint64_t)timestamp;
}

inline uint64_t get_ranged_random(uint64_t fr, uint64_t to) {
    // static std::default_random_engine engine(std::time(
    //     std::chrono::system_clock::now().time_since_epoch().count()));

    static std::default_random_engine engine(std::chrono::system_clock::now().time_since_epoch().count());
    static std::uniform_int_distribution<uint64_t> dist(fr, to);
    return dist(engine);
}
