#include "server.h"
#include "node.h"
#include <iostream>

Status RaftMessagesServiceImpl::RequestVote(ServerContext* context,
        const raft_messages::RequestVoteRequest* request,
        raft_messages::RequestVoteResponse* response)
{
    // When received RequestVoteRequest
    #if !defined(_HIDE_GRPC_NOTICE)
    debug("Receive RequestVoteRequest from Peer %s Name %s\n", context->peer().c_str(), request->name().c_str());
    #endif
    raft_messages::RequestVoteResponse response0 = raft_node->on_vote_request(*request);
    *response = response0;
    return Status::OK;
}

Status RaftMessagesServiceImpl::AppendEntries(ServerContext* context,
        const raft_messages::AppendEntriesRequest* request,
        raft_messages::AppendEntriesResponse* response)
{
    // When received AppendEntriesRequest
    #if !defined(_HIDE_HEARTBEAT_NOTICE) && !defined(_HIDE_GRPC_NOTICE)
    debug("Receive AppendEntriesRequest from Peer %s\n", context->peer().c_str());
    #endif
    raft_messages::AppendEntriesResponse response0 = raft_node->on_append_entries_request(*request);
    *response = response0;
    return Status::OK;
}

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
                raft_node->on_vote_response(call->response);
            }
        } else if (call_base->type == 2) {
            // This is a response to AppendEntries call
            AsyncClientCall<AppendEntriesResponse> * call =
                dynamic_cast<AsyncClientCall<AppendEntriesResponse> *>(call_base);
            if(call->status.ok()){
                // debug("Receive Async AppendEntriesResponse from Peer %s\n", call->context.peer().c_str());
                raft_node->on_append_entries_response(call->response);
            }
        }
        delete call_base;
    }
}


RaftMessagesClientAsync::RaftMessagesClientAsync(const char * addr, struct RaftNode * _raft_node) : raft_node(_raft_node) {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    debug("Create channel between host %s and remote %s\n", raft_node->name.c_str(), addr);
    stub = raft_messages::RaftMessages::NewStub(channel);
    cq_thread = std::thread(&RaftMessagesClientAsync::AsyncCompleteRpc, this);
    cq_thread.detach(); // `std::thread` destructs as the object is destroyed
}

RaftMessagesClientAsync::RaftMessagesClientAsync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesClientAsync(addr.c_str(), _raft_node) {

}


void RaftMessagesClientSync::AsyncRequestVote(const RequestVoteRequest& request)
{
    // A copy is needed
    std::thread t = std::thread([this](RequestVoteRequest r0){
        RequestVoteResponse response;
        ClientContext context;
        Status status = this->stub->RequestVote(&context, r0, &response);
        if (status.ok()) {
            this->raft_node->on_vote_response(response);
        } else {
            // std::cout << status.error_code() << ": " << status.error_message()
            //         << std::endl;
        }
    }, request);
    t.detach();
}

void RaftMessagesClientSync::AsyncAppendEntries(const AppendEntriesRequest& request)
{
    std::thread t = std::thread([this](AppendEntriesRequest r0){
        AppendEntriesResponse response;
        ClientContext context;
        Status status = this->stub->AppendEntries(&context, r0, &response);
        if (status.ok()) {
            this->raft_node->on_append_entries_response(response);
        } else {
        }
    }, request);
    t.detach();
}


RaftMessagesClientSync::RaftMessagesClientSync(const char * addr, struct RaftNode * _raft_node) : raft_node(_raft_node) {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    debug("Create channel between host %s and remote %s\n", raft_node->name.c_str(), addr);
    stub = raft_messages::RaftMessages::NewStub(channel);
}

RaftMessagesClientSync::RaftMessagesClientSync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesClientSync(addr.c_str(), _raft_node) {

}


RaftServerContext::RaftServerContext(struct RaftNode * node){
    service = new RaftMessagesServiceImpl(node);
    debug("Listen to %s\n", node->name.c_str());
    builder = new ServerBuilder();
    builder->AddListeningPort(node->name, grpc::InsecureServerCredentials());
    builder->RegisterService(service);
    server = std::unique_ptr<Server>{builder->BuildAndStart()};
}