/*************************************************************************
*  Nuft -- A C++17 Raft consensus algorithm library
*  Copyright (C) 2018  Calvin Neo 
*  Email: calvinneo@calvinneo.com;calvinneo1995@gmail.com
*  Github: https://github.com/CalvinNeo/Nuft/
*  
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*  
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*  
*  You should have received a copy of the GNU General Public License
*  along with this program.  If not, see <https://www.gnu.org/licenses/>.
**************************************************************************/

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

#include "Nuke/thread_pool.h"
#include "Nuke/lang_extend.h"
#include "Nuke/log.h"
#include "Nuke/string_utils.h"

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

struct RaftNodeLogger{
    struct RaftNode * node;
    void dolog(RaftNodeLogger & context, char const * file_name, char const * func_name, int line, int level, char const * fmt, va_list va);
};

