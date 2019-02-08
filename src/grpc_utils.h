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

#pragma once

#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "raft_messages.grpc.pb.h"
#include "raft_messages.pb.h"
#include "utils.h"
#include "settings.h"

using grpc::Server;
using grpc::Channel;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;
// using raft_messages::LogEntry;
// using raft_messages::AppendEntriesRequest;
// using raft_messages::AppendEntriesResponse;
// using raft_messages::RequestVoteRequest;
// using raft_messages::RequestVoteResponse;
// using raft_messages::RaftMessages

#define MONITOR_DELAY_THRES 150
inline bool monitor_delayed(uint64_t send_time){
    uint64_t delta = get_current_ms() - send_time;
    if(delta > MONITOR_DELAY_THRES){
        printf("GRPC: Delayed gRPC Call for %llu ms!!\n", delta);
        return true;
    }
    return false;
}

struct RaftMessagesServiceImpl : public raft_messages::RaftMessages::Service {
    // `RaftMessagesServiceImpl` defines what we do when receiving a RPC call.
    struct RaftNode * raft_node = nullptr;

    RaftMessagesServiceImpl(struct RaftNode * _raft_node) : raft_node(_raft_node) {

    }
    ~RaftMessagesServiceImpl(){
        raft_node = nullptr;
    }

    Status RequestVote(ServerContext* context, const raft_messages::RequestVoteRequest* request,
                       raft_messages::RequestVoteResponse* response) override;

    Status AppendEntries(ServerContext* context, const raft_messages::AppendEntriesRequest* request,
                         raft_messages::AppendEntriesResponse* response) override;

    Status InstallSnapshot(ServerContext* context, const raft_messages::InstallSnapshotRequest* request,
                            raft_messages::InstallSnapshotResponse* response) override;
};


#if defined(USE_GRPC_SYNC)
struct RaftMessagesClientSync : std::enable_shared_from_this<RaftMessagesClientSync>{
    // `RaftMessagesClientSync` defines how to make a sync RPC call, and how to handle its results.
    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
    using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
    using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;

    // Back reference to raft node.
    struct RaftNode * raft_node = nullptr;
    // std::shared_ptr<Nuke::ThreadExecutor> task_queue;
    Nuke::ThreadExecutor * task_queue = nullptr;
    std::string peer_name;

    void AsyncRequestVote(const RequestVoteRequest& request);
    void AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat);
    void AsyncInstallSnapshot(const InstallSnapshotRequest& request);

    RaftMessagesClientSync(const char * addr, struct RaftNode * _raft_node);
    RaftMessagesClientSync(const std::string & addr, struct RaftNode * _raft_node);
    void shutdown(){}
    bool is_shutdown(){return true;}

    ~RaftMessagesClientSync() {
        raft_node = nullptr;
    }
private:
    std::unique_ptr<raft_messages::RaftMessages::Stub> stub;
};


struct RaftMessagesStreamClientSync : std::enable_shared_from_this<RaftMessagesStreamClientSync> {
    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
    using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
    using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;
    // Back reference to raft node.
    struct RaftNode * raft_node = nullptr;
    // std::shared_ptr<Nuke::ThreadExecutor> task_queue;
    Nuke::ThreadExecutor * task_queue = nullptr;
    std::string peer_name;

    typedef grpc::ClientReaderWriter<RequestVoteRequest, RequestVoteResponse> CR;
    typedef grpc::ClientReaderWriter<AppendEntriesRequest, AppendEntriesResponse> CA;
    typedef grpc::ClientReaderWriter<InstallSnapshotRequest, InstallSnapshotResponse> CI;

    // CR * request_vote_stream = nullptr;
    // CA * append_entries_stream = nullptr;
    // CI * install_snapshot_stream = nullptr;
    std::shared_ptr<CR> request_vote_stream;
    std::shared_ptr<CA> append_entries_stream;
    std::shared_ptr<CI> install_snapshot_stream;
    ClientContext context_r;
    ClientContext context_a;
    ClientContext context_i;
    std::thread * t1 = nullptr;
    std::thread * t2 = nullptr;
    std::thread * t3 = nullptr;
    std::atomic<bool> stop;
    std::atomic<int> stop_count;

    void AsyncRequestVote(const RequestVoteRequest& request);
    void AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat);
    void AsyncInstallSnapshot(const InstallSnapshotRequest& request);
    void handle_response();
    void shutdown();
    bool is_shutdown();
    
    RaftMessagesStreamClientSync(const char * addr, struct RaftNode * _raft_node);
    RaftMessagesStreamClientSync(const std::string & addr, struct RaftNode * _raft_node);
    ~RaftMessagesStreamClientSync();
private:
    std::unique_ptr<raft_messages::RaftStreamMessages::Stub> stub;
};

#endif

#if defined(USE_GRPC_ASYNC)

struct RaftMessagesClientAsync {
    // `RaftMessagesClientAsync` defines how to make a async RPC call, and how to handle its results.
    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
    using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
    using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;

    // Back reference to raft node.
    struct RaftNode * raft_node;
    std::string peer_name;

    struct AsyncClientCallBase {
        char type;
        virtual ~AsyncClientCallBase() {}
    };
    template<typename T>
    struct AsyncClientCall : public AsyncClientCallBase {
        T response;
        ClientContext context;
        Status status;
        std::unique_ptr<ClientAsyncResponseReader<T>> response_reader;
        virtual ~AsyncClientCall() {}
    };

    void AsyncRequestVote(const RequestVoteRequest& request);
    void AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat);
    void AsyncInstallSnapshot(const InstallSnapshotRequest& request);

    void AsyncCompleteRpc();

    RaftMessagesClientAsync(const char * addr, struct RaftNode * _raft_node);
    RaftMessagesClientAsync(const std::string & addr, struct RaftNode * _raft_node);
    ~RaftMessagesClientAsync() {
        raft_node = nullptr;
        cq.Shutdown();
    }
private:
    std::unique_ptr<raft_messages::RaftMessages::Stub> stub;
    CompletionQueue cq;
    std::thread cq_thread;
};
#endif

struct RaftServerContext{
    RaftMessagesServiceImpl * service;
    std::unique_ptr<Server> server;
    ServerBuilder * builder;
    RaftServerContext(struct RaftNode * node);
    std::thread wait_thread;
    ~RaftServerContext();
};



struct RaftMessagesStreamServiceImpl : public raft_messages::RaftStreamMessages::Service {
    // `RaftMessagesServiceImpl` defines what we do when receiving a RPC stream call.

    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
    using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
    using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;

    struct RaftNode * raft_node = nullptr;
    std::atomic<bool> stop;

    RaftMessagesStreamServiceImpl(struct RaftNode * _raft_node) : raft_node(_raft_node) {
        stop.store(false);
    }
    ~RaftMessagesStreamServiceImpl(){
        debug("GRPC: RaftMessagesStreamServiceImpl Destruct\n");
        raft_node = nullptr;
    }

    Status RequestVote(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::RequestVoteResponse, RequestVoteRequest>* stream);
    Status AppendEntries(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::AppendEntriesResponse, AppendEntriesRequest>* stream);
    Status InstallSnapshot(ServerContext* context, ::grpc::ServerReaderWriter< ::raft_messages::InstallSnapshotResponse, InstallSnapshotRequest>* stream);
};


struct RaftStreamServerContext{
    std::shared_ptr<RaftMessagesStreamServiceImpl> service;
    // RaftMessagesStreamServiceImpl * service;
    std::shared_ptr<Server> server;
    ServerBuilder * builder;
    RaftStreamServerContext(struct RaftNode * node);
    std::thread wait_thread;
    ~RaftStreamServerContext();
};
