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

#include "server.h"
#include "node.h"
#include <iostream>

void RaftMessagesClientAsync::AsyncCompleteRpc()
{
    void * got_tag;
    bool ok = false;
    while (this->cq.Next(&got_tag, &ok))
    {
        AsyncClientCallBase * call_base = static_cast<AsyncClientCallBase *>(got_tag);
        if (call_base->type == 1) {
            // This is a response to RequestVote call
            AsyncClientCall<RequestVoteResponse> * call =
                dynamic_cast<AsyncClientCall<RequestVoteResponse> *>(call_base);
            if(call->status.ok()){
                // debug("Receive Async RequestVoteResponse from Peer %s\n", call->context.peer().c_str());
                if(raft_node) raft_node->on_vote_response(call->response);
            }
        } else if (call_base->type == 2) {
            // This is a response to AppendEntries call
            AsyncClientCall<AppendEntriesResponse> * call =
                dynamic_cast<AsyncClientCall<AppendEntriesResponse> *>(call_base);
            if(call->status.ok()){
                // debug("Receive Async AppendEntriesResponse from Peer %s\n", call->context.peer().c_str());
                // TODO Replace true by real value of heartbeat
                if(raft_node) raft_node->on_append_entries_response(call->response, true);
            }
        }else if (call_base->type == 3) {
            // This is a response to InstallSnapshot call
            AsyncClientCall<InstallSnapshotResponse> * call =
                dynamic_cast<AsyncClientCall<InstallSnapshotResponse> *>(call_base);
            if(call->status.ok()){
                if(raft_node) raft_node->on_install_snapshot_response(call->response);
            }
        }

        delete call_base;
    }
}



RaftMessagesClientAsync::RaftMessagesClientAsync(const char * addr, struct RaftNode * _raft_node) : raft_node(_raft_node), peer_name(addr) {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    debug("Create channel from host %s to remote %s\n", raft_node->name.c_str(), addr);
    stub = raft_messages::RaftMessages::NewStub(channel);
    cq_thread = std::thread(&RaftMessagesClientAsync::AsyncCompleteRpc, this);
    cq_thread.detach(); 
}

RaftMessagesClientAsync::RaftMessagesClientAsync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesClientAsync(addr.c_str(), _raft_node) {

}
