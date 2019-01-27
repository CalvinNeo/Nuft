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

#include "grpc_utils.h"
#include "node.h"
#include <iostream>

RaftServerContext::RaftServerContext(struct RaftNode * node){
    service = new RaftMessagesServiceImpl(node);
#if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Listen to %s\n", node->name.c_str());
#endif
    builder = new ServerBuilder();
    builder->AddListeningPort(node->name, grpc::InsecureServerCredentials());
    builder->RegisterService(service);
    server = std::unique_ptr<Server>{builder->BuildAndStart()};
    wait_thread = std::thread([&](){server->Wait();});
}
RaftServerContext::~RaftServerContext(){
    debug("Wait shutdown\n");
    server->Shutdown();
    wait_thread.join();
    debug("wait_thread Joined\n");
    delete service;
    delete builder;
}

Status RaftMessagesServiceImpl::RequestVote(ServerContext* context,
        const raft_messages::RequestVoteRequest* request,
        raft_messages::RequestVoteResponse* response)
{
    // When received RequestVoteRequest
    #if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Receive RequestVoteRequest from Peer %s Name %s\n", context->peer().c_str(), request->name().c_str());
    #endif
    if(!raft_node){
        return Status::OK;
    }
    int response0 = raft_node->on_vote_request(response, *request);
    if(response0 == 0){
        return Status::OK;
    }else{
        // ref grpc::StatusCode.
        // https://grpc.io/grpc/cpp/grpcpp_2impl_2codegen_2status__code__enum_8h_source.html
        return Status(grpc::StatusCode::UNAVAILABLE, "Peer is not ready for this request.");
    }
}

Status RaftMessagesServiceImpl::AppendEntries(ServerContext* context,
        const raft_messages::AppendEntriesRequest* request,
        raft_messages::AppendEntriesResponse* response)
{
    // When received AppendEntriesRequest
    #if !defined(_HIDE_HEARTBEAT_NOTICE) && !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Receive AppendEntriesRequest from Peer %s\n", context->peer().c_str());
    #endif
    if(!raft_node){
        return Status::OK;
    }
    int response0 = raft_node->on_append_entries_request(response, *request);
    if(response0 == 0){
        return Status::OK;
    }else{
        return Status(grpc::StatusCode::UNAVAILABLE, "Peer is not ready for this request.");
    }
}

Status RaftMessagesServiceImpl::InstallSnapshot(ServerContext* context,
        const raft_messages::InstallSnapshotRequest* request,
        raft_messages::InstallSnapshotResponse* response)
{
    // When received AppendEntriesRequest
    #if !defined(_HIDE_HEARTBEAT_NOTICE) && !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Receive InstallSnapshot from Peer %s\n", context->peer().c_str());
    #endif
    if(!raft_node){
        return Status::OK;
    }
    int response0 = raft_node->on_install_snapshot_request(response, *request);
    if(response0 == 0){
        return Status::OK;
    }else{
        return Status(grpc::StatusCode::UNAVAILABLE, "Peer is not ready for this request.");
    }
}


