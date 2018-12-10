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

struct RaftMessagesServiceImpl : public raft_messages::RaftMessages::Service {
    // `RaftMessagesServiceImpl` defines what we do when receiving a RPC call.
    struct RaftNode * raft_node = nullptr;

    RaftMessagesServiceImpl(struct RaftNode * _raft_node) : raft_node(_raft_node) {

    }

    Status RequestVote(ServerContext* context, const raft_messages::RequestVoteRequest* request,
                       raft_messages::RequestVoteResponse* response) override;

    Status AppendEntries(ServerContext* context, const raft_messages::AppendEntriesRequest* request,
                         raft_messages::AppendEntriesResponse* response) override;

};


struct RaftMessagesClientSync {
    // `RaftMessagesClientSync` defines how to make a sync RPC call, and how to handle its results.
    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;

    // Back reference to raft node.
    struct RaftNode * raft_node = nullptr;
    std::shared_ptr<Nuke::ThreadExecutor> task_queue;

    void AsyncRequestVote(const RequestVoteRequest& request);
    void AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat);

    RaftMessagesClientSync(const char * addr, struct RaftNode * _raft_node);
    RaftMessagesClientSync(const std::string & addr, struct RaftNode * _raft_node);
    ~RaftMessagesClientSync() {

    }
private:
    std::unique_ptr<raft_messages::RaftMessages::Stub> stub;
};



struct RaftMessagesClientAsync {
    // `RaftMessagesClientAsync` defines how to make a async RPC call, and how to handle its results.
    using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
    using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
    using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
    using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;

    // Back reference to raft node.
    struct RaftNode * raft_node = nullptr;

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

    void AsyncRequestVote(const RequestVoteRequest& request)
    {
        // Call will be removed from CompletionQueue
        // debug("GRPC sending AppendRequestVote\n");
        AsyncClientCall<RequestVoteResponse> * call = new AsyncClientCall<RequestVoteResponse>();
        call->type = 1;
        call->response_reader = stub->AsyncRequestVote(&call->context, request, &cq);
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void AsyncAppendEntries(const AppendEntriesRequest& request)
    {
        // debug("GRPC sending AppendEntries\n");
        AsyncClientCall<AppendEntriesResponse> * call = new AsyncClientCall<AppendEntriesResponse>();
        call->type = 2;
        call->response_reader = stub->AsyncAppendEntries(&call->context, request, &cq);
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void AsyncCompleteRpc();

    RaftMessagesClientAsync(const char * addr, struct RaftNode * _raft_node);
    RaftMessagesClientAsync(const std::string & addr, struct RaftNode * _raft_node);
    ~RaftMessagesClientAsync() {

    }
private:
    std::unique_ptr<raft_messages::RaftMessages::Stub> stub;
    CompletionQueue cq;
    std::thread cq_thread;
};


struct RaftServerContext{
    RaftMessagesServiceImpl * service;
    std::unique_ptr<Server> server;
    ServerBuilder * builder;
    RaftServerContext(struct RaftNode * node);
    ~RaftServerContext(){
        debug("Server destructed.\n");
        delete service;
        delete builder;
    }
};
