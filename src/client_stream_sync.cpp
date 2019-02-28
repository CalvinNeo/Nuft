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

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;

// See https://groups.google.com/forum/#!topic/grpc-io/G7FzRNQBWhU
// and https://grpc.io/grpc/cpp/classgrpc_1_1_client_reader.html

#if defined(USE_GRPC_SYNC) && defined(USE_GRPC_STREAM)
RaftMessagesStreamClientSync::RaftMessagesStreamClientSync(const char * addr, struct RaftNode * _raft_node) : raft_node(_raft_node), peer_name(addr) {
    stop.store(false);
    stop_count.store(0);
    std::shared_ptr<Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
#if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Create stream channel between host %s and remote %s\n", raft_node->name.c_str(), addr);
#endif
    stub = raft_messages::RaftStreamMessages::NewStub(channel);
    request_vote_stream = std::move(stub->RequestVote(&context_r));
    append_entries_stream = std::move(stub->AppendEntries(&context_a));
    install_snapshot_stream = std::move(stub->InstallSnapshot(&context_i));
    // request_vote_stream = new CR(stub->RequestVote(&context));
    // append_entries_stream = new CA(stub->AppendEntries(&context));
    // install_snapshot_stream = new CI(stub->InstallSnapshot(&context));
}

void RaftMessagesStreamClientSync::shutdown(){
    // request_vote_stream->WritesDone();
    // append_entries_stream->WritesDone();
    // install_snapshot_stream->WritesDone();
    stop.store(true);
    context_r.TryCancel();
    context_a.TryCancel();
    context_i.TryCancel();
}


bool RaftMessagesStreamClientSync::is_shutdown(){
    return stop_count.load() == 3;
}

RaftMessagesStreamClientSync::~RaftMessagesStreamClientSync() {
    // debug("Client Cancel.\n");
    if(t1 && t1->joinable()){
        t1->join();
    }
    // debug("Client t1 Join.\n");
    if(t2 && t2->joinable()){
        t2->join();
    }
    // debug("Client t2 Join.\n");
    if(t3 && t3->joinable()){
        t3->join();
    }
    // debug("Client t3 Join.\n");
    delete t1;
    t1 = nullptr;
    delete t2;
    t2 = nullptr;
    delete t3;
    t3 = nullptr;
    
    Status status;
    // NOTICE https://nanxiao.me/en/be-careful-of-using-grpc-clientstreaminginterface-finish-function/
    // Before calling finish, make sure `Read()` returns false.

    // status = request_vote_stream->Finish();
    // debug("Client R Finished.\n");
    // status = append_entries_stream->Finish();
    // debug("Client A Finished.\n");
    // status = install_snapshot_stream->Finish();
    // debug("Client I Finished.\n");
    raft_node = nullptr;
    // delete request_vote_stream;
    // delete append_entries_stream;
    // delete install_snapshot_stream;
}

void RaftMessagesStreamClientSync::handle_response(){
    // NOTICE We can't use shared_from_this inside constructor.
    // So we have `handle_response`
    auto strongThis = shared_from_this();
    t1 = new std::thread([this](){
        std::string peer_name = this->peer_name;
        RequestVoteResponse response;
        while(!stop.load()){
            while (1) {
                context_r.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
                if(!this->request_vote_stream->Read(&response)){
                    break;
                }
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv RequestVoteResponse %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
                if(stop.load()){
                    break;
                }
                if(!this->raft_node){
                    #if !defined(_HIDE_GRPC_NOTICE)
                    debug("GRPC: Old Message, Response RaftNode destructed.\n");
                    #endif
                    continue;
                }
                if(Nuke::contains(this->raft_node->peers, peer_name)){
                    if(response.time() < this->raft_node->start_timepoint){
                        debug("GRPC: Old message, Response from previous request REJECTED.\n");
                    }else{
                    //     monitor_delayed(request.time());
                        this->raft_node->on_vote_response(response);
                    }
                }
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv RequestVoteResponse End %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
            }
        }
        debug("GRPC: RequestVoteResponse Join %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
        stop_count.fetch_add(1);
    });
    t2 = new std::thread([this](){
        std::string peer_name = this->peer_name;
        AppendEntriesResponse response;
        while(!stop.load()){
            while (1) {
                // printf("Try Read %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                context_a.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
                if(!this->append_entries_stream->Read(&response)){
                    break;
                }
                // printf("Try Read End %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv AppendEntriesResponse from %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
                if(stop.load()){
                    break;
                }
                if(!this->raft_node){
                    #if !defined(_HIDE_GRPC_NOTICE)
                    debug("GRPC: Old Message, Response RaftNode destructed.\n");
                    #endif
                    continue;
                }
                if(Nuke::contains(this->raft_node->peers, peer_name)){
                    if(response.time() < this->raft_node->start_timepoint){
                        debug("GRPC: Old message, Response from previous request REJECTED.\n");
                    }else{
                        // NOTICE Deadlock may happen when:
                        // 1. `on_append_entries_response()` requires lock hold by `remove_peer()`(main thread)
                        // 2. `remove_peer()`(main thread) waits `t2->join()`
                        // See stream.CrashNode.block.log
                    //     monitor_delayed(request.time());
                        this->raft_node->on_append_entries_response(response, false);
                    }
                }
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv AppendEntriesResponse End %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
            }
            // printf("Timeout Exit %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
        }
        debug("GRPC: AppendEntriesResponse Join %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
        stop_count.fetch_add(1);
    });
    t3 = new std::thread([this](){
        std::string peer_name = this->peer_name;
        InstallSnapshotResponse response;
        while(!stop.load()){
            while (1) {
                context_i.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
                if(!this->install_snapshot_stream->Read(&response)){
                    break;
                }
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv InstallSnapshotResponse from %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
                if(stop.load()){
                    break;
                }
                if(!this->raft_node){
                    #if !defined(_HIDE_GRPC_NOTICE)
                    debug("GRPC: Old Message, Response RaftNode destructed.\n");
                    #endif
                    continue;
                }
                if(Nuke::contains(this->raft_node->peers, peer_name)){
                    if(response.time() < this->raft_node->start_timepoint){
                        debug("GRPC: Old message, Response from previous request REJECTED.\n");
                    }else{
                    //     monitor_delayed(request.time());
                        this->raft_node->on_install_snapshot_response(response);
                    }
                }
                #if !defined(_HIDE_GRPC_NOTICE)
                debug("GRPC: Recv InstallSnapshotResponse End %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
                #endif
            }
        }
        debug("GRPC: InstallSnapshotResponse Join %s->%s\n", peer_name.c_str(), this->raft_node->name.c_str());
        stop_count.fetch_add(1);
    });
}

RaftMessagesStreamClientSync::RaftMessagesStreamClientSync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesStreamClientSync(addr.c_str(), _raft_node) {

}

void RaftMessagesStreamClientSync::AsyncRequestVote(const RequestVoteRequest& request){
    std::string peer_name = this->peer_name;
    #if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Send RequestVoteRequest from %s to %s\n", request.name().c_str(), peer_name.c_str());
    #endif
    if(!stop.load()){
        request_vote_stream->Write(request);
        // NOTICE Peer may already closed
        // assert(request_vote_stream->Write(request) != 0);
    }
    // debug("GRPC: Send RequestVoteRequest from %s to %s end\n", request.name().c_str(), peer_name.c_str());

}
void RaftMessagesStreamClientSync::AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat){
    #if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Send AppendEntriesRequest from %s to %s\n", request.name().c_str(), peer_name.c_str());
    #endif
    if(!stop.load()){
        append_entries_stream->Write(request);
        // assert(append_entries_stream->Write(request) != 0);
    }
}
void RaftMessagesStreamClientSync::AsyncInstallSnapshot(const InstallSnapshotRequest& request){
    if(!stop.load()){
        install_snapshot_stream->Write(request);
        // assert(install_snapshot_stream->Write(request) != 0);
    }
}

#endif