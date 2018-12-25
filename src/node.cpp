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

void Persister::Dump(bool backup_conf){
    // TODO maybe use SerializeToString is better.
    if(!node->is_running()) return;
    std::fstream fo{(node->name + std::string(".persist")).c_str(), std::ios::binary | std::ios::out};
    raft_messages::PersistRecord record;
    record.set_term(node->current_term);
    // assert(node->current_term != 77777);
    record.set_name(node->name);
    record.set_vote_for(node->vote_for);
    for(int i = 0; i < node->logs.size(); i++){
        raft_messages::LogEntry & entry = *(record.add_entries());
        entry = node->logs[i];
    }
    if(node->trans_conf){
        raft_messages::ConfRecord confrecord;
        confrecord.set_peers(RaftNode::Configuration::to_string(*node->trans_conf));
        confrecord.set_index(node->trans_conf->index);
        confrecord.set_index2(node->trans_conf->index2);
        confrecord.set_state(node->trans_conf->state);
        *record.mutable_conf_record() = confrecord;
    }else{
        raft_messages::ConfRecord confrecord;
        confrecord.set_peers(RaftNode::Configuration::to_string(RaftNode::Configuration{{}, {}, node->peers, node->name}));
        confrecord.set_index(-1);
        confrecord.set_index2(-1);
        confrecord.set_state(RaftNode::Configuration::State::BLANK);
        *record.mutable_conf_record() = confrecord;
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
    if(record.has_conf_record()){
        const raft_messages::ConfRecord & confrecord = record.conf_record();
        RaftNode::Configuration conf = RaftNode::Configuration::from_string(confrecord.peers());
        if(confrecord.state() == RaftNode::Configuration::State::BLANK){
            node->reset_peers();
            for(int i = 0; i < conf.old.size(); i++){
                if(conf.old[i] != node->name){
                    node->add_peer(conf.old[i]);
                }
            }
        }else{
            if(node->trans_conf) delete node->trans_conf;
            node->trans_conf = new RaftNode::Configuration(conf);
            node->trans_conf->state = confrecord.state();
            node->trans_conf->index = confrecord.index();
            node->trans_conf->index2 = confrecord.index2();
            node->reset_peers();
            node->apply_conf(*node->trans_conf);
        }
    }
    fo.close();
}
