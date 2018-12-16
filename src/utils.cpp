#include "utils.h"
#include "node.h"

void RaftNodeLogger::dolog(RaftNodeLogger & context, char const * file_name, char const * func_name, int line, int level, char const * fmt, va_list va){
    char * buf;
    uint64_t current_ms = get_current_ms();
    Nuke::asprintf(&buf, "RaftLog[%d, %llu](%s:%d@%s) [Me=%s,Leader=%s]: %s", level, current_ms % 10000, file_name, line, func_name, 
            context.node->name.c_str(), context.node->leader_name.c_str(), fmt);
    std::vfprintf(stdout, buf, va);
    fflush(stdout);
    delete buf;
}
