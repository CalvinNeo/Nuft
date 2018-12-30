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
    debug("Joined\n");
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
    int response0 = raft_node->on_install_snapshot_request(response, *request);
    if(response0 == 0){
        return Status::OK;
    }else{
        return Status(grpc::StatusCode::UNAVAILABLE, "Peer is not ready for this request.");
    }
}


void RaftMessagesClientSync::AsyncRequestVote(const RequestVoteRequest& request)
{
    // A copy of `request` is needed
    // TODO Replace `std::thread` implementation with future.then implementation. 
    std::string peer_name = this->peer_name;
    auto strongThis = shared_from_this();
#if defined(USE_GRPC_SYNC_BARE)
    std::thread t = std::thread(
#else
    task_queue->add_task("V" + request.name() + peer_name,
#endif
    [strongThis, request, peer_name](){ 
        RequestVoteResponse response;
        ClientContext context;
        Status status = strongThis->stub->RequestVote(&context, request, &response);
        if (status.ok()) {
            if(!strongThis->raft_node){
                debug("Response RaftNode destructed.\n");
                return;
            }
            // Test whether raft_node_>peers[peer_name] is destructed.
            if(Nuke::contains(strongThis->raft_node->peers, peer_name)){
                if(request.time() < strongThis->raft_node->start_timepoint){
                    debug("Response from previous request REJECTED.\n");
                }else
                    strongThis->raft_node->on_vote_response(response);
            }
        } else {
            // #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC error(RequestVote %s->%s) %d: %s\n", request.name().c_str(), peer_name.c_str(), status.error_code(), status.error_message().c_str());
            // #endif
        }
    }
#if defined(USE_GRPC_SYNC_BARE)
    , request);
    t.detach();
#else
    );
#endif
}

void RaftMessagesClientSync::AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat)
{
    std::string peer_name = this->peer_name;
    auto strongThis = shared_from_this();
#if defined(USE_GRPC_SYNC_BARE)
    std::thread t = std::thread(
#else
    task_queue->add_task("A" + request.name() + peer_name,
#endif
    [strongThis, heartbeat, request, peer_name](){
        AppendEntriesResponse response;
        ClientContext context;
        Status status = strongThis->stub->AppendEntries(&context, request, &response);
        if (status.ok()) {
            if(!strongThis->raft_node){
                debug("Response RaftNode destructed.\n");
                return;
            }
            if(Nuke::contains(strongThis->raft_node->peers, peer_name)){
                if(request.time() < strongThis->raft_node->start_timepoint){
                    debug("Response from previous request REJECTED.\n");
                }else
                    strongThis->raft_node->on_append_entries_response(response, heartbeat);
            }
        } else {
            // #if !defined(_HIDE_GRPC_NOTICE)
            if(!heartbeat) debug("GRPC error(RequestVote %s->%s) %d: %s\n", request.name().c_str(), peer_name.c_str(), status.error_code(), status.error_message().c_str());
            // #endif
        }
    }
#if defined(USE_GRPC_SYNC_BARE)
    , request);
    t.detach();
#else
    );
#endif
}

void RaftMessagesClientSync::AsyncInstallSnapshot(const InstallSnapshotRequest& request)
{
    std::string peer_name = this->peer_name;
    struct RaftNode * raft_node = this->raft_node;
#if defined(USE_GRPC_SYNC_BARE)
    std::thread t = std::thread(
#else
    task_queue->add_task(
#endif
    [this, request, peer_name, raft_node](){
        InstallSnapshotResponse response;
        ClientContext context;
        Status status = this->stub->InstallSnapshot(&context, request, &response);
        if (status.ok()) {
            if(Nuke::contains(raft_node->peers, peer_name)){
                this->raft_node->on_install_snapshot_response(response);
            }
        } else {
            #if !defined(_HIDE_GRPC_NOTICE)
            if(!heartbeat) debug("GRPC error %d: %s\n", status.error_code(), status.error_message().c_str());
            #endif
        }
    }
#if defined(USE_GRPC_SYNC_BARE)
    , request);
    t.detach();
#else
    );
#endif
}


RaftMessagesClientSync::RaftMessagesClientSync(const char * addr, struct RaftNode * _raft_node) : raft_node(_raft_node), peer_name(addr) {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
#if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Create channel between host %s and remote %s\n", raft_node->name.c_str(), addr);
#endif
    stub = raft_messages::RaftMessages::NewStub(channel);
}

RaftMessagesClientSync::RaftMessagesClientSync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesClientSync(addr.c_str(), _raft_node) {

}
