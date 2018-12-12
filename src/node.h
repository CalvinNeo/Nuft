#pragma once

#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <map>
#include <functional>
#include "utils.h"
#include "server.h"


typedef std::string NodeName;
typedef int64_t IndexID;
typedef uint64_t TermID;

static constexpr IndexID default_index_cursor = -1; // When no log, index is -1
static constexpr TermID default_term_cursor = 0; // When starting, term is 0
static constexpr uint64_t default_timeout_interval_lowerbound = 1000;
static constexpr uint64_t default_timeout_interval_upperbound = 1800;
static constexpr uint64_t default_heartbeat_interval = 30;
static constexpr uint64_t default_election_fail_timeout_interval = 3000; // 
#define vote_for_none ""

#define GUARD
#define GUARD std::lock_guard<std::mutex> guard((mut));
#define LOCK mut.lock();
#define UNLOCK mut.unlock();

struct Log {

};

#if defined(USE_GRPC_ASYNC)
#undef USE_GRPC_SYNC
#define USE_GRPC_ASYNC
// Use Async gRPC model
typedef RaftMessagesClientAsync RaftMessagesClient;
#else
// Use Sync gRPC model
#undef USE_GRPC_ASYNC
#define USE_GRPC_SYNC
typedef RaftMessagesClientSync RaftMessagesClient;
#define GRPC_SYNC_CONCUR_LEVEL 5
#endif


struct NodePeer {
    NodeName name;
    bool voted_for_me = false;
    // Index of next log to copy
    IndexID next_index = default_index_cursor;
    // Index of logs already copied
    IndexID match_index = default_index_cursor;
    // Use `RaftMessagesClient` to send RPC message to peer.
    RaftMessagesClient * raft_message_client = nullptr;
    bool running = true;
};

enum NUFT_RESULT{
    NUFT_OK = 0,
    NUFT_FAIL = 1,
    NUFT_RETRY = 2,
    NUFT_NOT_LEADER = 3,
    NUFT_RESULT_SIZE,
};
enum NUFT_CB_TYPE {
    NUFT_CB_ELECTION_START,
    NUFT_CB_ELECTION_END,
    NUFT_CB_STATE_CHANGE,
    NUFT_CB_ON_APPLY,
    NUFT_CB_CONF_START,
    NUFT_CB_CONF_END,
    NUFT_CB_SIZE,
};
struct NuftCallbackArg{
    struct RaftNode * node;
    int a1 = 0;
    int a2 = 0;
};
typedef int NuftResult;
// typedef NuftResult NuftCallbackFunc(NUFT_CB_TYPE, NuftCallbackArg *);
// typedef std::function<NuftResult(NUFT_CB_TYPE, NuftCallbackArg *)> NuftCallbackFunc;
#define NuftCallbackFunc std::function<NuftResult(NUFT_CB_TYPE, NuftCallbackArg *)>

enum NUFT_CONF_STATE{
    // ref liblogcabin/Raft/RaftConsensus.h, Configuation::State
    NUFT_CONF_BLANK,
    NUFT_CONF_STABLE,
    NUFT_CONF_STAGING,
    NUFT_CONF_TRANS,
};

struct RaftNode {
    enum NodeState {
        Follower = 0,
        Candidate = 1,
        Leader = 2,
        NotRunning = 3,
    };

    enum ElectionState{
        ELE_NONE, // other state
        // How the node detect its election period starts?
        ELE_NEW, // start election itself
        ELE_AGA, // re-start election itself
        ELE_VOTE, // detect other node's election
        // How the node detect its election ends?
        ELE_SUC, // win election 
        ELE_T, // my election is timeout
        ELE_FAIL, // lose election as a candidate
        ELE_SUC_OB, // observed other's election success
    }; // Only used in callbacks to suggest an event related to election(its or others) has happend.

    const char * node_state_name(NodeState state) const{
        switch(state){
            case NodeState::Follower:
                return "Follower";
            case NodeState::Candidate:
                return "Candidate";
            case NodeState::Leader:
                return "Leader";
            case NodeState::NotRunning:
                return "NotRunning";
            default:
                return "Error";
        }
    }

    // Standard
    TermID current_term = default_term_cursor;
    NodeName vote_for = vote_for_none;
    std::vector<raft_messages::LogEntry> logs;
    std::string leader_name;

    // RSM usage
    IndexID commit_index = default_index_cursor;
    IndexID last_applied = default_index_cursor;

    // Leader exclusive

    // Node infomation
    NodeName name;
    NodeState state;
    bool paused = false;
    uint64_t last_tick = 0; // Reset every `default_heartbeat_interval`
    uint64_t elect_timeout_due = 0; // At this point the Follower start election
    uint64_t election_fail_timeout_due = 0; // At this point the Candidate's election timeout

    // Candidate exclusive
    size_t vote_got = 0;

    // Network
    std::map<std::string, NodePeer> peers;
    RaftServerContext * raft_message_server = nullptr;
    std::thread timer_thread;

    // RPC utils for sync
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
    // All peer's RaftMessagesClient share one `sync_client_task_queue`.
    std::shared_ptr<Nuke::ThreadExecutor> sync_client_task_queue;
#endif

    // Utils
    // Multiple thread can access the same object, use mutex for protection.
    std::mutex mut;
    bool tobe_destructed = false;

    // API
    NuftCallbackFunc callbacks[NUFT_CB_SIZE];
    bool debugging = false;

    NuftResult invoke_callback(NUFT_CB_TYPE type, NuftCallbackArg arg){
        if(callbacks[type] != nullptr){
            return (callbacks[type])(type, &arg);
        }
        return -NUFT_FAIL;
    }

    IndexID last_log_index() const {
        return logs.size() == 0 ? default_index_cursor : logs.back().index();
    }
    IndexID last_log_term() const {
        return logs.size() == 0 ? default_term_cursor : logs.back().term();
    }

    size_t cluster_size() const {
        return peers.size() + 1;
    }

    bool enough_votes() const {
        // If I got enough votes
        return enough_votes(vote_got);
    }
    bool enough_votes(size_t vote) const {
        // Is `vote` votes enough
        return vote > (peers.size() + 1) / 2;
    }

    const NodeName get_leader_name() const{
        // This function helps Clients to locate Leader.
        // Keep in mind that in raft, Clients only communicate with Leader.
        if(state == NodeState::Leader){
            return name;
        }
        // There's no Leader.
        return leader_name;
    }

    void on_timer() {
        // Check heartbeat
        uint64_t current_ms = get_current_ms();

        if (!is_running()) return;
        if ((state == NodeState::Leader) && current_ms >= default_heartbeat_interval + last_tick) {
            // Leader's routine heartbeat
            send_heartbeat();
            last_tick = current_ms;
        }
        // Check if leader is timeout
        if ((state == NodeState::Follower || state == NodeState::Candidate)
                && (current_ms >= elect_timeout_due)
                ) { 
            // Start election(from a Follower), or restart a election(from a Candidate)
            do_election();
            // In `do_election`, `leader_name` is set to "".
            if(state == NodeState::Follower){
                invoke_callback(NUFT_CB_ELECTION_START, {this, ELE_NEW});
            }else{
                invoke_callback(NUFT_CB_ELECTION_START, {this, ELE_AGA});
            }
        }

        // Check if election is timeout
        if ((state == NodeState::Candidate)
                && (current_ms >= election_fail_timeout_due)
                ) {
            // In this case, no consensus is reached during the previous election,
            // We restart a new election.
            if (enough_votes()) {
                // Maybe there is only me in the cluster...
                become_leader();
                invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_SUC});
            } else {
                debug("Election failed(timeout) with %u votes at term %lu.\n", vote_got, current_term);
                GUARD
                reset_election_timeout();
                invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_T});
            }
        }
    }

    void start() {
        debug("Start node. Switch state to Follower.\n");
        if (state != NodeState::NotRunning) {
            return;
        }
        GUARD
        raft_message_server = new RaftServerContext(this);
        state = NodeState::Follower;
        reset_election_timeout();
    }

    void stop() {
        debug("Stop node of %s\n", name.c_str());
        paused = true;
    }

    void resume() {
        paused = false;
        debug("Resume node of %s\n", name.c_str());
    }

    void stop_ignore_peer(const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].running = true;
        }
    }
    void ignore_peer(const std::string & peer_name){
        // Only for debug usage.
        // Ignore RPC request from peer.
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].running = false;
        }
    }
    bool is_running() const{
        return (state != NodeState::NotRunning) && (!paused);
    }
    bool is_running(const std::string & peer_name) const{
        if(!Nuke::contains(peers, peer_name)) return false;
        return peers[peer_name].running;
    }

    void reset_election_timeout() {
        uint64_t current_ms = get_current_ms();
        uint64_t delta = get_ranged_random(default_timeout_interval_lowerbound, default_timeout_interval_upperbound);
        elect_timeout_due = current_ms + delta;
        #if !defined(_HIDE_HEARTBEAT_NOTICE)
        debug("Sched election after %llums = %llu(+%llu), current %llu. State %s\n", 
                delta, elect_timeout_due, elect_timeout_due - current_ms, current_ms, node_state_name(state));
        #endif
        // `election_fail_timeout_due` should be set only after election begins,
        // Otherwise it will keep triggered in `on_time()`
        election_fail_timeout_due = UINT64_MAX;
    }

    void add_peer(const std::string & peer_name) {
        if (Nuke::contains(peers, peer_name)) {
            debug("Peer already found.\n");
        } else {
            peers[peer_name] = NodePeer{peer_name};
            peers[peer_name].running = true;
            peers[peer_name].raft_message_client = new RaftMessagesClient(peer_name, this);
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
            peers[peer_name].raft_message_client->task_queue = sync_client_task_queue;
#endif
        }
    }

    void do_apply() {

    }

    void switch_to(NodeState new_state) {
        NodeState o = state;
        state = new_state;
        invoke_callback(NUFT_CB_STATE_CHANGE, {this, o});
    }

    void send_heartbeat() {
        // TODO: optimize for every peer:
        //      If already sent logs, don't do heartbeat this time.
        do_append_entries(true);
    }

    void become_follower(TermID new_term) {
        if (state == NodeState::Leader) {
            // A splitted leader got re-connection
            // TODO: must stop heartbeat immediately
        } else {
            // Lose the election

        }
        switch_to(NodeState::Follower);
        current_term = new_term;
        vote_for = vote_for_none;
    }

    void become_leader() {
        debug("Now I(%s) am the Leader of term %llu, because I got %llu votes.\n", name.c_str(), current_term, vote_got);
        switch_to(NodeState::Leader);
        vote_for = vote_for_none;
        leader_name = name;
        for (auto & pp : this->peers) {
            // Actually we don't know how many logs have been copied,
            // So we assume no log are copied to peer.
            pp.second.match_index = default_index_cursor;
            // "When a leader first comes to power,
            // it initializes all nextIndex values to the index just after the
            // last one in its log."
#if !defined(USE_COMMIT_INDEX)
            pp.second.next_index = logs.size();
#else
            // This strategy is from Mushroom's implementation.
            pp.second.next_index = commit_index + 1;
#endif
        }

        // Heartbeat my leadership
        // Do not call `do_append_entries` or cause deadlock.
        last_tick = 0;
    }

    void do_election() {
        GUARD
        
        switch_to(NodeState::Candidate);
        current_term++;
        vote_got = 1; // Vote for myself
        vote_for = name;

        uint64_t current_ms = get_current_ms();
        election_fail_timeout_due = current_ms + default_election_fail_timeout_interval;
        debug("Lost Leader of (%s). Start election term: %lu, will timeout in %llu ms.\n", leader_name.c_str(), current_term, default_election_fail_timeout_interval);
        leader_name = "";
        // Same reason for setting `elect_timeout_due`. Ref. `reset_election_timeout`
        elect_timeout_due = UINT64_MAX;

        for (auto & pp : peers) {
            raft_messages::RequestVoteRequest request;
            request.set_name(name);
            request.set_term(current_term);
            request.set_last_log_index(last_log_index());
            request.set_last_log_term(last_log_term());
            
            NodePeer & peer = pp.second;
            pp.second.voted_for_me = false;
            debug("Send RequestVoteRequest from %s to %s\n", name.c_str(), peer.name.c_str());
            assert(peer.raft_message_client != nullptr);
            peer.raft_message_client->AsyncRequestVote(request);
        }
    }

    int on_vote_request(raft_messages::RequestVoteResponse * response_ptr, const raft_messages::RequestVoteRequest & request) {
        // When receive a `RequestVoteRequest` from peer.

        std::string peer_name = request.name();
        if((!is_running()) || (!is_running(peer_name))){
        #if !defined(_HIDE_PAUSED_NODE_NOTICE)
            debug("Ignore RequestVoteRequest from %s, because I(%s)/peer is not running.\n", request.name().c_str(), name.c_str());
        #endif
            return -1;
        }
        GUARD
        
        leader_name = "";
            
        raft_messages::RequestVoteResponse & response = *response_ptr;
        response.set_name(name);
        response.set_vote_granted(false);

        if (request.term() > current_term) {
            // An election is ongoing. 
            debug("Become Follower(%s): Receive RequestVoteRequest from %s with new term of %llu, me %llu.\n", name.c_str(), request.name().c_str(), request.term(), current_term);
            // We can't set `vote_for` to none here, or will cause multiple Leader elected(ref strange_failure3.log)
            // Error: vote_for = vote_for_none;
            // TODO An optimation from the Raft Paper Chapter 7 Issue 3: 
            // "removed servers can disrupt the cluster. These servers will not receive heartbeats, so they will time out and start new elections.
            // They will then send RequestVote RPCs with new term numbers, and this will cause the current leader to revert to follower state."
            
            become_follower(request.term());
            // We can't assume the sender has won the election.
            // Error: leader_name = request.name();
            reset_election_timeout();
            invoke_callback(NUFT_CB_ELECTION_END, {this, state==NodeState::Candidate?ELE_FAIL:ELE_SUC_OB});
        } else if (request.term() < current_term) {
            // Reject. A request of old term
            debug("Reject RequestVoteRequest because it is from a old term %llu, me %llu.\n", request.term(), current_term);
            goto end;
        }

        if (vote_for != vote_for_none && vote_for != request.name()) {
            // Reject. Already vote other
            debug("Reject RequestVoteRequest because I already vote for %s.\n", vote_for.c_str());
            goto end;
        }

        if (request.last_log_term() < last_log_term()) {
            debug("Reject RequestVoteRequest because request.last_log_term() < last_log_term(): %llu < %llu.\n", request.last_log_term(), last_log_term());
            goto end;
        }
        if (request.last_log_term() == last_log_term() && request.last_log_index() < last_log_index()) {
            debug("Reject RequestVoteRequest because request.last_log_index() < last_log_index(): %llu < %llu.\n", request.last_log_index(), last_log_index());
            goto end;
        }

        // Vote.
        vote_for = request.name();
        response.set_vote_granted(true);
end:
        // Must set term here, cause `become_follower` may change it
        response.set_term(current_term);
        invoke_callback(NUFT_CB_ELECTION_START, {this, ELE_VOTE, response.vote_granted()});
        debug("Respond from %s to %s with %s\n", name.c_str(), request.name().c_str(), response.vote_granted() ? "YES" : "NO");
        return 0;
    }

    void on_vote_response(const raft_messages::RequestVoteResponse & response) {
        // Called from server.h
        // When a candidate receives vote from others

        GUARD
        if (state != NodeState::Candidate) {
            return;
        }

        if(!Nuke::contains(peers, response.name())){
            // debug("Receive vote from unregisted %s = %s\n", response.name().c_str(), response.vote_granted() ? "YES" : "NO");
            return;
        }
        // `operator[]` will create automaticlly
        NodePeer & peer = peers[response.name()];

        debug("Receive vote from %s = %s\n", response.name().c_str(), response.vote_granted() ? "YES" : "NO");
        if ((!peer.voted_for_me) && response.vote_granted()) {
            // If peer grant vote for me for the FIRST time.
            peer.voted_for_me = true;
            vote_got ++;
        }
        if (enough_votes()) {
            assert(response.term() <= current_term);
            become_leader();
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_SUC});
        } else if (response.term() > current_term) {
            // Found newer term from response. My election definitely fail.
            // However, we don't know who's the leader yet, and the Leader may not exist now,
            // because this may be new election.
            become_follower(response.term());
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_FAIL});
        }
    }

    void do_append_entries(bool heartbeat) {
        // Send `AppendEntriesRequest` to all peer.
        // When heartbeat is set to true, this function will NOT send logs, even if there are some updates.
        // If you believe the logs are updated and need to be notified to peers, then you must set `heartbeat` to false.

        GUARD
        for (auto & pp : this->peers) {
            NodePeer & peer = pp.second;
            // We copy log entries to peer, from `prev_log_index`.
            // This may fail when "an existing entry conflicts with a new one (same index but different terms)"
            IndexID prev = peer.next_index - 1;
            IndexID prev_log_index = prev;
            TermID prev_log_term = prev > default_index_cursor ? logs[prev].term() : default_term_cursor;
            raft_messages::AppendEntriesRequest request;
            request.set_name(name);
            request.set_term(current_term);
            request.set_prev_log_index(prev_log_index);
            request.set_prev_log_term(prev_log_term);
            request.set_leader_commit(commit_index);
            if (true || !heartbeat) {
                // Add entries to request
                // NOTICE Even we set heartbeat to true, we still need to check entries.
                // Because some nodes may suffer from network failure etc., 
                // and fail to update their log when we firstly sent entries in `do_log`.
                if(peer.next_index < logs.size()){
#if !defined(_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST)
                    debug("Copy to peer %s from %llu to %llu\n", peer.name.c_str(), peer.next_index, logs.size());
#endif
                }
                for (IndexID i = peer.next_index; i < logs.size(); i++) {
                    raft_messages::LogEntry & entry = *(request.add_entries());
                    entry = logs[i];
                }
            }

            #if defined(_HIDE_HEARTBEAT_NOTICE)
            if(heartbeat){}else
            #endif
            debug("Begin sending %s AppendEntriesRequest to %s.\n", heartbeat ? "heartbeat": "normal", peer.name.c_str());
            peer.raft_message_client->AsyncAppendEntries(request, heartbeat);
        }
    }

    int on_append_entries_request(raft_messages::AppendEntriesResponse * response_ptr, const raft_messages::AppendEntriesRequest & request) {
        // When a Follower/Candidate receive `AppendEntriesResponse` from Leader,
        // Try append entries, then return a `AppendEntriesResponse`.
        std::string peer_name = request.name();
        if((!is_running()) || (!is_running(peer_name))){
            #if !defined(_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST) && !defined(_HIDE_PAUSED_NODE_NOTICE)
            #if defined(_HIDE_HEARTBEAT_NOTICE)
            if(request.entries().size() == 0){}else
            #endif
            debug("Ignore AppendEntriesRequest from %s by %s, because {%s/%s} is not running.\n", 
                    request.name().c_str(), name.c_str(), paused?"I":"", 
                    !Nuke::contains(peers, peer_name)||!peers[peer_name].running?"peer":"");
            #endif
            return -1;
        }
        GUARD
        raft_messages::AppendEntriesResponse & response = *response_ptr;
        response.set_name(name);
        response.set_success(false);

        IndexID prev_i = request.prev_log_index(), prev_j = 0;

        // Leader is still alive
        reset_election_timeout();

        if (request.term() < current_term) {
            debug("Reject AppendEntriesRequest from %s, because it has older term %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
            goto end;
        }

        if (request.term() > current_term) {
            // Discover a request with newer term.
            debug("Become Follower: Receive AppendEntriesRequest from %s with newer term %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
            become_follower(request.term());
            leader_name = request.name();
            // Observe the emergence of a new Leader.
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_SUC_OB});
        }
        if (request.term() == current_term && state == NodeState::Follower && leader_name == ""){
            // In this case, this node is a Follower, maybe through `RequestVoteRequest`.
            // Now new leader is elected and not knowing the elected leader.
            // This function is mostly for debugging/API, in order to trigger callback, otherwise the debugging thread may block forever at `cv.wait()`
            // TODO Whether the Follower will increase term when receiving RequestVote from a Candidate?
            become_follower(request.term());
            leader_name = request.name();
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_SUC_OB});
        }
        if (request.term() == current_term && state == NodeState::Candidate) {
            // In election and a Leader has won. 
            debug("Become Follower: Receive AppendEntriesRequest from %s with term %llu. I lose election of my candidate term.\n", request.name().c_str(), request.term());
            become_follower(request.term());
            leader_name = request.name();
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_FAIL});
        }
        // if(debugging){printf("DDDDDDDDDDDDDDDDDDDDD request %llu current %llu KKK %d leader_name %s\n", request.term(), current_term, state, leader_name.c_str());}


        // `request.prev_log_index() == -1` happens at the very beginning when a Leader with no Log, and send heartbeats to others.
        if (request.prev_log_index() >= 0 && request.prev_log_index() >= logs.size()) {
            // I don't actually have the `prev_log_index()`,
            // Failed.
            debug("AppendEntries fail because I(%s) don't have prev_log_index = %llu, my log size = %llu.\n", name.c_str(), request.prev_log_index(), logs.size());
            goto end;
        }

        if (request.prev_log_index() >= 0 && logs[request.prev_log_index()].term() != request.prev_log_term()) {
            // "If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it"
            assert(commit_index < request.prev_log_index());
            debug("AppendEntries fail because my(%s) term[prev_log_index=%lld] = %llu, Leader's = %llu.\n", name.c_str(), request.prev_log_index(),
                logs[request.prev_log_index()].term(), request.prev_log_term());
            logs.erase(logs.begin() + request.prev_log_index(), logs.end());
            goto end;
        }

        prev_i++;
        for (; prev_i < logs.size() && prev_j < request.entries_size(); prev_i++, prev_j++) {
            // Remove all entries not meet with `request.entries` after `prev_i`
            if (logs[prev_i].term() != request.entries(prev_j).term()) {
                assert(commit_index < prev_i);
                logs.erase(logs.begin() + prev_i, logs.end());
                break;
            }
        }
        if(request.entries_size() > 0){
            debug("Node %s AppendEntries from %lld to %lld.\n", name.c_str(), prev_j, prev_j + request.entries_size());
        }
        if (prev_j < request.entries_size()) {
            // Copy the rest entries
            logs.insert(logs.end(), request.entries().begin() + prev_j, request.entries().end());
        }

        if (request.leader_commit() > commit_index) {
            // Update commit
            commit_index = std::min(request.leader_commit(), IndexID(logs.size()) - 1);
            debug("Node %s advance commit to %lld.\n", name.c_str(), commit_index);
        }else{
            // debug("Node %s can't advance commit, remain %lld.\n", name.c_str(), commit_index);
        }

succeed:
        // debug("AppendEntries Succeed.\n");
        response.set_success(true);
end:
        // Must set term here, because `become_follower` may change it
        response.set_term(current_term);
        response.set_last_log_index(last_log_index());
        response.set_last_log_term(last_log_term());
        return 0;
    }

    void on_append_entries_response(const raft_messages::AppendEntriesResponse & response, bool heartbeat) {
        // When Leader receive `AppendEntriesResponse` from others

        GUARD

        if (state != NodeState::Leader) {
            return;
        }

        if (response.term() > current_term) {
            // I know I am not the Leader by now.
            // However, I don't know who is the Leader yet.
            // This is most likely a recovering from network partitioning.
            // ref `on_vote_response`.
            become_follower(response.term());
        } else if (response.term() != current_term) {
            // Reject. Invalid term
            return;
        }
        NodePeer & peer = peers[response.name()];
        peer.next_index = response.last_log_index() + 1;
        peer.match_index = response.last_log_index();

        if (!response.success()) {
            // A failed `AppendEntriesRequest` can certainly not lead to `commit_index` updating
            debug("Peer %s returns AppendEntriesResponse: FAILED\n", response.name().c_str());
            return;
        }
        // Update commit
        if (commit_index >= response.last_log_index()) {
            // `commit_index` still ahead of peer's index.
            // This peer can't contribute to `commit_vote`
            if(!heartbeat) debug("Leader %s: My commit_index(%lld) is ahead of peer's(%lld). Peer(%s) grants NO commit vote.\n",
                    name.c_str(), commit_index, response.last_log_index(), peer.name.c_str());
            return;
        }
        // See `on_append_entries_request`
        assert(logs[response.last_log_index()].term() == response.last_log_term());
        if (current_term != logs[response.last_log_index()].term()) {
            // According to <<In Search of an Understandable Consensus Algorithm>>
            // "Raft never commits log entries from previous terms by counting replicas.
            // Only log entries from the leader’s current term are committed by counting replicas"
            if(!heartbeat) debug("Both commit_index(%lld) agrees. However term disagree(me:%llu, peer:%llu). Peer grants NO commit vote.\n", commit_index, current_term, logs[response.last_log_index()].term());
            return;
        }

        // Now we can try to advance `commit_index` to `response.last_log_index()`
        size_t commit_vote = 1;
        for (auto & pp : peers) {
            if (pp.second.match_index >= response.last_log_index()) {
                commit_vote++;
            }
        }
        // "A log entry is committed once the leader
        // that created the entry has replicated it on a majority of
        // the servers..."
        if (enough_votes(commit_vote)) {
            if(!heartbeat) debug("Leader %s Advance commit_index from %lld to %lld.\n", name.c_str(), commit_index, response.last_log_index());
            commit_index = response.last_log_index();
            // When the entry has been safely replicated, the leader applies the entry to its state machine and returns the result of that execution to the client.
            do_apply();
        }else{
            if(!heartbeat) debug("Can't advance commit index because of inadequate votes of %u.\n", commit_vote);
        }
    }

    NuftResult get_log(IndexID index, ::raft_messages::LogEntry & l) {
        GUARD
        if(index < logs.size()){
            l = logs[index];
            return NUFT_OK;
        }
        return -NUFT_FAIL;
    }

    NuftResult do_log(::raft_messages::LogEntry entry){
        LOCK
        if(state != NodeState::Leader){
            // No, I am not the Leader, so please find the Leader first of all.
            debug("Not Leader!!!!!\n");
            return -NUFT_NOT_LEADER;
        }
        IndexID index = logs.size();
        entry.set_term(this->current_term);
        entry.set_index(index);
        entry.set_command(0);
        
        logs.push_back(entry);
        UNLOCK
        debug("Append LOCAL log Index %lld, Term %llu, commit_index %lld. Now copy to peers.\n", index, current_term, commit_index);
        // "The leader appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel..."
        do_append_entries(false);
        return NUFT_OK;
    }

    bool do_log(const std::string & log_string){
        ::raft_messages::LogEntry entry;
        entry.set_data(log_string);
        return this->do_log(entry);
    }

    void update_configuration(const std::vector<std::string> & append, const std::vector<std::string> & remove){
        if(state != NodeState::Leader || paused){
            return ;
        }
        if(Nuke::contains(remove, name)){
        }
    }

    RaftNode(const std::string & addr) : state(NodeState::NotRunning), name(addr) {
        std::memset(callbacks, 0, sizeof callbacks);
        using namespace std::chrono_literals;
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
        sync_client_task_queue = std::make_shared<Nuke::ThreadExecutor>(GRPC_SYNC_CONCUR_LEVEL);
#endif
        timer_thread = std::thread([&]() {
            while (1) {
                if(tobe_destructed){
                    return;
                }
                this->on_timer();
                std::this_thread::sleep_for(10ms);
            }
        });
    }
    RaftNode(const RaftNode &) = delete;
    ~RaftNode() {
        debug("Destruct RaftNode %s.\n", name.c_str());
        tobe_destructed = true;
        state = NodeState::NotRunning;
        timer_thread.join();
        for (auto & pp : this->peers) {
            NodePeer & peer = pp.second;
            delete (peer.raft_message_client);
        }
        delete raft_message_server;
    }

    void _list_peers(){
        for (auto & pp : this->peers) {
            printf("==> %s\n", pp.second.name.c_str());
        }
    }
};

RaftNode * make_raft_node(const std::string & addr);
