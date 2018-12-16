#include <functional>
#include <cstdarg>

namespace Nuke{

struct BaseLogger{
    static auto get_new_fmt(){
        static char new_fmt[1024];
        return new_fmt;
    }
    BaseLogger(){
    }
    void dolog(BaseLogger & context, char const * file_name, char const * func_name, int line, int level, char const * fmt, va_list va){
        std::snprintf(BaseLogger::get_new_fmt(), 1024, "%s:%d(In function %s): LOG[%d] %s\n", file_name, line, func_name, level, fmt);
        std::vfprintf(stdout, BaseLogger::get_new_fmt(), va);
        fflush(stdout);
    }
    ~BaseLogger(){}
};

template <typename CONTEXT>
struct _log_struct{
    typedef std::function<void(CONTEXT &, char const *, va_list)> call_type;
    operator call_type () const{
        return [&](CONTEXT & context, char const * fmt, va_list va){
            context.dolog(context, file_name, func_name, line, level, fmt, va);
        };
    }
    _log_struct(const char * _file_name, const char * _func_name, int _line, int _level)
        : file_name(_file_name), func_name(_func_name), line(_line), level(_level) {

    }
    const char* file_name;
    const char* func_name;
    int line;
    int level;
};

template <typename CONTEXT>
void _log(_log_struct<CONTEXT> f, CONTEXT context, char const *fmt, ...){
    va_list va;
    va_start(va, fmt);
    ((typename _log_struct<CONTEXT>::call_type)f)(context, fmt, va);
    va_end(va);
}

}

#define NUKE_LOG(LOGLEVEL, context, fmt, ...) Nuke::_log(Nuke::_log_struct<decltype(context)>{__FILE__, __FUNCTION__, __LINE__, LOGLEVEL}, context, fmt, ##__VA_ARGS__)
