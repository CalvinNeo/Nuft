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

using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;
    
RaftServerContext::RaftServerContext(struct RaftNode * node){
    service = new RaftMessagesServiceImpl(node);
#if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Listen to %s\n", node->name.c_str());
#endif
    builder = new ServerBuilder();
    builder->AddListeningPort(node->name, grpc::InsecureServerCredentials());
    builder->RegisterService(service);
    server = std::unique_ptr<Server>{builder->BuildAndStart()};
}
RaftServerContext::~RaftServerContext(){
    debug("Wait shutdown\n");
    server->Shutdown();
    debug("Wait Join\n");
    wait_thread = std::thread([&](){
        server->Wait();
    });
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

Status RaftMessagesStreamServiceImpl::RequestVote(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::RequestVoteResponse, RequestVoteRequest>* stream){
    while(!stop.load()){
        while (1) {
            RequestVoteRequest request;
            RequestVoteResponse response;
            // context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
            if(!stream->Read(&request)){
                break;
            }
            if(stop.load()){
                break;
            }
            assert(request.has_name());
            #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC: Receive RequestVoteRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
            if(!raft_node){
                // `raft_node` is cleared, 
                debug("GRPC: Me %s shutdown\n", request.name().c_str(), raft_node->name.c_str());
                return Status::OK;
            }
            // debug("GRPC: Handle RequestVoteRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            int response0 = raft_node->on_vote_request(&response, request);
            if(response0 == 0){
                assert(response.has_name());
                stream->Write(response);
            }
            // debug("GRPC: Finish RequestVoteRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC: Respond RequestVoteRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
        }
    }
    return Status::OK;
}
Status RaftMessagesStreamServiceImpl::AppendEntries(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::AppendEntriesResponse, AppendEntriesRequest>* stream){
    while(!stop.load()){
        while (1) {
            AppendEntriesRequest request;
            AppendEntriesResponse response;
            // context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
            if(!stream->Read(&request)){
                break;
            }
            if(stop.load()){
                break;
            }
            assert(request.has_name());
            #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC: Receive AppendEntriesRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
            if(!raft_node){
                debug("GRPC: Me %s shutdown\n", request.name().c_str(), raft_node->name.c_str());
                return Status::OK;
            }
            int response0 = raft_node->on_append_entries_request(&response, request);
            if(response0 == 0){
                assert(response.has_name());
                stream->Write(response);
            }
            #if !defined(_HIDE_GRPC_NOTICE)
            // debug("GRPC: Respond AppendEntriesRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
        }
    }
    return Status::OK;
}
Status RaftMessagesStreamServiceImpl::InstallSnapshot(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::InstallSnapshotResponse, InstallSnapshotRequest>* stream){
    while(!stop.load()){
        while (1) {
            InstallSnapshotRequest request;
            InstallSnapshotResponse response;
            // context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
            if(!stream->Read(&request)){
                break;
            }
            if(stop.load()){
                break;
            }
            assert(request.has_name());
            #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC: Receive InstallSnapshotRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
            if(!raft_node){
                debug("GRPC: Me %s shutdown\n", request.name().c_str(), raft_node->name.c_str());
                return Status::OK;
            }
            int response0 = raft_node->on_install_snapshot_request(&response, request);
            if(response0 == 0){
                assert(response.has_name());
                stream->Write(response);
            }
            #if !defined(_HIDE_GRPC_NOTICE)
            // debug("GRPC: Respond InstallSnapshotRequest from %s, me %s\n", request.name().c_str(), raft_node->name.c_str());
            #endif
        }
    }

    return Status::OK;
}

RaftStreamServerContext::RaftStreamServerContext(struct RaftNode * node){
    service = std::make_shared<RaftMessagesStreamServiceImpl>(node);
    // service = new RaftMessagesStreamServiceImpl(node);
#if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Listen to %s\n", node->name.c_str());
#endif
    builder = new ServerBuilder();
    builder->AddListeningPort(node->name, grpc::InsecureServerCredentials());
    builder->RegisterService(service.get());
    // builder->RegisterService(service);
    server = std::move(std::unique_ptr<Server>{builder->BuildAndStart()});
}
RaftStreamServerContext::~RaftStreamServerContext(){
    service->stop.store(true);
    wait_thread = std::thread([service=service, server=server](){
        server->Wait();
    });
    debug("Wait shutdown\n");
    // https://grpc.io/grpc/cpp/classgrpc_1_1_server_interface.html#a6a1d337270116c95f387e0abf01f6c6c
    // In the case of the sync API, if the RPC function for a streaming call has already been started and takes a week to complete, 
    // the RPC function won't be forcefully terminated
    // server->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
    // server->Shutdown();
    debug("Wait Join/Detach\n");
    // NOTICE We can't join here
    wait_thread.detach();
    delete builder;
    debug("wait_thread Joined/Detached\n");
}