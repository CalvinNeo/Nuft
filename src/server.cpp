#include "server.h"
#include "node.h"
#include <iostream>
Status RaftMessagesServiceImpl::RequestVote(ServerContext* context,
        const raft_messages::RequestVoteRequest* request,
        raft_messages::RequestVoteResponse* response)
{
    // When received RequestVoteRequest
    #if !defined(_HIDE_GRPC_NOTICE)
    debug("GRPC: Receive RequestVoteRequest from Peer %s Name %s\n", context->peer().c_str(), request->name().c_str());
    #endif
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
    int response0 = raft_node->on_append_entries_request(response, *request);
    if(response0 == 0){
        return Status::OK;
    }else{
        return Status(grpc::StatusCode::UNAVAILABLE, "Peer is not ready for this request.");
    }
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
                // TODO Replace true by real value of heartbeat
                raft_node->on_append_entries_response(call->response, true);
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
    cq_thread.detach(); // `std::thread` destructs as the object is destroyed
}

RaftMessagesClientAsync::RaftMessagesClientAsync(const std::string & addr, struct RaftNode * _raft_node) : RaftMessagesClientAsync(addr.c_str(), _raft_node) {

}


void RaftMessagesClientSync::AsyncRequestVote(const RequestVoteRequest& request)
{
    // A copy of `request` is needed
    // TODO Replace `std::thread` implementation with future.then implementation. 
    std::string peer_name = this->peer_name;
    struct RaftNode * raft_node = this->raft_node;
#if defined(USE_GRPC_SYNC_BARE)
    std::thread t = std::thread(
#else
    task_queue->add_task(
#endif
    [this, request, peer_name, raft_node](){ 
        RequestVoteResponse response;
        ClientContext context;
        Status status = this->stub->RequestVote(&context, request, &response);
        if (status.ok()) {
            // Test whether raft_node_>peers[peer_name] is destructed.
            if(Nuke::contains(raft_node->peers, peer_name)){
                this->raft_node->on_vote_response(response);
            }
        } else {
            #if !defined(_HIDE_GRPC_NOTICE)
            debug("GRPC error %d: %s\n", status.error_code(), status.error_message().c_str());
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

void RaftMessagesClientSync::AsyncAppendEntries(const AppendEntriesRequest& request, bool heartbeat)
{
    std::string peer_name = this->peer_name;
    struct RaftNode * raft_node = this->raft_node;
#if defined(USE_GRPC_SYNC_BARE)
    std::thread t = std::thread(
#else
    task_queue->add_task(
#endif
    [this, heartbeat, request, peer_name, raft_node](){
        AppendEntriesResponse response;
        ClientContext context;
        Status status = this->stub->AppendEntries(&context, request, &response);
        if (status.ok()) {
            if(Nuke::contains(raft_node->peers, peer_name)){
                this->raft_node->on_append_entries_response(response, heartbeat);
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
