#pragma once
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <algorithm>
#include <vector>

namespace Nuke{

inline int asprintf(char ** ptr, const char * fmt, ...) {
    size_t fmt_len = std::strlen(fmt);
    int size = fmt_len * 2 + 50;
    *ptr = new char[size];
    va_list ap;
    while (1) {     // Maximum two passes on a POSIX system...
        va_start(ap, fmt);
        int n = vsnprintf(*ptr, size, fmt, ap);
        va_end(ap);
        if (n > -1 && n < size) {
            // Notice that only when this returned value is non-negative and less than n,
            // the string has been completely written.
            return n;
        }else{
            delete [] *ptr;
            size *= 2;
            *ptr = new char[size];
        }
    }
    return -1;
}

inline std::vector<std::string> split(const std::string & s, const std::string & splitter){
    std::vector<std::string> res;
    size_t start = 0;
    size_t p;
    while((p = s.find(splitter, start)) != std::string::npos){
        res.push_back(std::string(s.begin()+start, s.begin()+p));
        start = p + splitter.size();
    }
    res.push_back(std::string(s.begin()+start, s.end()));
    if(res.size() == 1 && res[0] == ""){
        return {};
    }
    return res;
}

template<typename Iter>
inline std::string join(Iter b, Iter e, const std::string & combine){
    return std::accumulate(b, e, std::string(""), [&](const auto & x, const auto & y){return x == "" ? y : x + combine + y;});
}
template<typename Iter, typename F>
inline std::string join(Iter b, Iter e, const std::string & combine, F f){
    std::string s;
    for(auto i = b; i != e; i++){
        if(i != b){
            s += combine;
        }
        s += f(*i);
    }
    return s;
}

}
