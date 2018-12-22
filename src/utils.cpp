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
