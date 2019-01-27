#include <map>
#include <vector>
#include <algorithm>

namespace Nuke{
template<typename C, typename K>
bool contains(const C & c, const K & key) {
    return std::find(c.begin(), c.end(), key) != c.end();
}

template<typename K, typename V>
bool contains(const std::map<K, V> & mp, const K & key) {
    typename std::map<K, V>::const_iterator iter = mp.find(key);
    return iter != mp.end();
}

template<typename K>
bool in(const K & k, std::vector<K> vec){
    return std::find(vec.begin(), vec.end(), k) != vec.end();
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
