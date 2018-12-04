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

namespace Nuke {

template<typename T, typename K>
bool exists(T mp, K key) {
    typename T::const_iterator iter = mp.find(key);
    return iter != mp.end();
}
inline void trim(std::string & line) {
    auto val = line.find_last_not_of(" \n\r\t") + 1;

    if (val == line.size() || val == std::string::npos) {
        val = line.find_first_not_of(" \n\r\t");
        line = line.substr(val);
    }
    else {
        line.erase(val);
    }
}

struct defer
{
public:
    ~defer(){
        while (!fstack.empty()){
            fstack.back()();
            fstack.pop_back();
        }
    }
    void push(std::function<void()> func){
        fstack.push_back(func);
    }
protected:
    std::vector<std::function<void()>> fstack;
};

}