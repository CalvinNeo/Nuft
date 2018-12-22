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

#include "node.h"
#include "server.h"
#include <fstream>

RaftNode * make_raft_node(const std::string & addr) {
    RaftNode * node = new RaftNode(addr);
    return node;
}

void Persister::Dump(){
    // TODO maybe use SerializeToString is better.
    std::fstream fo{(node->name + std::string(".persist")).c_str(), std::ios::binary | std::ios::out};
    raft_messages::PersistRecord record;
    record.set_term(node->current_term);
    record.set_name(node->name);
    record.set_vote_for(node->vote_for);
    for(int i = 0; i < node->logs.size(); i++){
        raft_messages::LogEntry & entry = *(record.add_entries());
        entry = node->logs[i];
    }
    record.SerializeToOstream(&fo);
    fo.close();
}
void Persister::Load(){
    std::fstream fo{(node->name + std::string(".persist")).c_str(), std::ios::binary | std::ios::in};
    raft_messages::PersistRecord record;
    record.ParseFromIstream(&fo);
    node->current_term = record.term();
    node->name = record.name();
    node->vote_for = record.vote_for();
    node->logs.clear();
    for(int i = 0; i < record.entries_size(); i++){
        node->logs.push_back(record.entries(i));
    }
    fo.close();
}
