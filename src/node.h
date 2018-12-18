#pragma once

#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <map>
#include <functional>
#include <algorithm>
#include "utils.h"
#include "server.h"
#include <tuple>

#define debug_node(...) NUKE_LOG(LOGLEVEL_DEBUG, RaftNodeLogger{this}, ##__VA_ARGS__)
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
    // Debug usage
    bool receive_enabled = true;
    bool send_enabled = true;
    // According to the Raft Paper Chapter 6 Issue 1,
    // Newly added nodes is in staging mode, and thet have no voting rights,
    // Until they are sync-ed.
    bool voting_rights = true;
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
    NUFT_CB_ON_REPLICATE,
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
typedef std::function<NuftResult(NUFT_CB_TYPE, NuftCallbackArg *)> NuftCallbackFunc;


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

    const char * node_state_name(NodeState s) const{
        switch(s){
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

    struct Configuration{
        static Configuration from_string(const std::string & conf_str){
            std::vector<std::string> v1 = Nuke::split(conf_str, "\n");
            std::vector<std::string> app = Nuke::split(v1[0], ";");
            std::vector<std::string> rem = Nuke::split(v1[1], ";");
            std::vector<std::string> old = Nuke::split(v1[2], ";");
            return {app, rem, old};
        }
        static std::string to_string(const Configuration & conf){
            std::string conf_str;
            conf_str += Nuke::join(conf.app.begin(), conf.app.end(), ";") + "\n";
            conf_str += Nuke::join(conf.rem.begin(), conf.rem.end(), ";") + "\n";
            conf_str += Nuke::join(conf.old.begin(), conf.old.end(), ";") + "\n";
            return conf_str; 
        }
        Configuration(const std::vector<std::string> & a, const std::vector<std::string> & r, const std::vector<NodeName> & o) : app(a), rem(r), old(o){
            
        }
		Configuration(const std::vector<std::string> & a, const std::vector<std::string> & r, const std::map<NodeName, NodePeer> & o, const NodeName & leader_exclusive) : app(a), rem(r){
            for(auto && p: o){
                old.push_back(p.first);
            }
            old.push_back(leader_exclusive);
            oldvote_thres = old.size(); // Including myself
            newvote_thres = oldvote_thres;
            for(auto & s: app){
                if(!Nuke::contains(old, s)){
                    newvote_thres++;
                }
            }
            for(auto & s: rem){
                if(Nuke::contains(old, s)){
                    newvote_thres--;
                }
            }
        }
        std::vector<std::string> app;
        std::vector<std::string> rem;
        std::vector<std::string> old;
        // If `commit_index >= index`, then C_{old,new} is comitted.
        IndexID index = -1;
        // If `commit_index >= index2`, then C_{new} Configuration is comitted.
        IndexID index2 = -1;
        size_t oldvote_thres = 0;
        size_t newvote_thres = 0;
        enum State{
            // This state means `trans_conf == nullptr`.
            BLANK,
            // In staging period, no entries are logged.
            STAGING,
            // Now entry for joint consensus is logged, but not committed.
            OLD_JOINT,
            // Now entry for joint consensus is committed.
            JOINT,
            // Now entry for new configuration are sent.
            // ref update_configuration_new
            JOINT_NEW,
            NEW,
        };
        State state = STAGING;
        bool is_in_old(const std::string & peer_name) const {
            return Nuke::contains(old, peer_name); 
        }
        bool is_in_new(const std::string & peer_name) const {
            return (is_in_old(peer_name) && !Nuke::contains(rem, peer_name)) || Nuke::contains(app, peer_name);
        }
        bool is_in_joint(const std::string & peer_name) const{
            return is_in_old(peer_name) || is_in_new(peer_name);
        }
        void init(){
        }
    };

    // Standard
    TermID current_term = default_term_cursor;
    NodeName vote_for = vote_for_none;
    std::vector<raft_messages::LogEntry> logs;
    std::string leader_name;
    struct Configuration * trans_conf = nullptr;

    // RSM usage
    IndexID commit_index = default_index_cursor;
    IndexID last_applied = default_index_cursor;

    // Leader exclusive

    // Node infomation
    NodeName name;
    NodeState state;
    bool paused = false;
    bool tobe_destructed = false;
    uint64_t last_tick = 0; // Reset every `default_heartbeat_interval`
    uint64_t elect_timeout_due = 0; // At this point the Follower start election
    uint64_t election_fail_timeout_due = 0; // At this point the Candidate's election timeout

    // Candidate exclusive
    size_t vote_got = 0;

    // Network
    std::map<NodeName, NodePeer> peers;
    RaftServerContext * raft_message_server = nullptr;
    std::thread timer_thread;

    // RPC utils for sync
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
    // All peer's RaftMessagesClient share one `sync_client_task_queue`.
    std::shared_ptr<Nuke::ThreadExecutor> sync_client_task_queue;
#endif

    // Utils
    // Multiple thread can access the same object, use mutex for protection.
    mutable std::mutex mut;

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

    bool enough_votes(size_t vote) const {
        // Is `vote` votes enough
		// TODO votes can be cached.
		size_t voters = compute_votes_thres();
        return vote > voters / 2;
    }
    size_t compute_votes_thres() const {
        size_t voters = 1;
        for(auto & pr: peers){
            if(pr.second.voting_rights){
                voters ++;
            }
        }
		return voters;
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
            GUARD
            send_heartbeat(guard);
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
            if (enough_votes(vote_got)) {
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
        debug_node("Start node. Switch state to Follower.\n");
        if (state != NodeState::NotRunning) {
            return;
        }
        raft_message_server = new RaftServerContext(this);
        state = NodeState::Follower;
        reset_election_timeout();
    }

    void stop() {
        debug_node("Stop node of %s\n", name.c_str());
        paused = true;
    }
    void resume() {
        paused = false;
        debug_node("Resume node of %s\n", name.c_str());
    }
    bool is_running() const{
        return (state != NodeState::NotRunning) && (!paused);
    }

    void enable_receive(const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].receive_enabled = true;
        }
    }
    void disable_receive(const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].receive_enabled = false;
        }
    }
    void enable_send(const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].send_enabled = true;
        }
    }
    void disable_send(const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers[peer_name].send_enabled = false;
        }
    }
    bool is_peer_receive_enabled(const std::string & peer_name) const{
        if(!Nuke::contains(peers, peer_name)) return false;
        return peers[peer_name].receive_enabled;
    }
    bool is_peer_send_enabled(const std::string & peer_name) const{
        if(!Nuke::contains(peers, peer_name)) return false;
        return peers[peer_name].send_enabled;
    }

    void reset_election_timeout() {
        uint64_t current_ms = get_current_ms();
        uint64_t delta = get_ranged_random(default_timeout_interval_lowerbound, default_timeout_interval_upperbound);
        elect_timeout_due = current_ms + delta;
        #if !defined(_HIDE_HEARTBEAT_NOTICE)
        debug_node("Sched election after %llums = %llu(+%llu), current %llu. State %s\n", 
                delta, elect_timeout_due, elect_timeout_due - current_ms, current_ms, node_state_name(state));
        #endif
        // `election_fail_timeout_due` should be set only after election begins,
        // Otherwise it will keep being triggered in `on_time()`
        election_fail_timeout_due = UINT64_MAX;
    }

    NodePeer & add_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name) {
        //TODO Bad idea to return this reference, use `unique_ptr` later
        if (Nuke::contains(peers, peer_name)) {
            debug_node("Peer %s already found.\n", peer_name.c_str());
        } else {
            peers[peer_name] = NodePeer{peer_name};
            peers[peer_name].send_enabled = true;
            peers[peer_name].receive_enabled = true;
            peers[peer_name].raft_message_client = new RaftMessagesClient(peer_name, this);
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
            peers[peer_name].raft_message_client->task_queue = sync_client_task_queue;
#endif
        }
        return peers[peer_name];
    }
    void remove_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        // Must under the protection of mutex
        if(Nuke::contains(peers, peer_name)){
            debug_node("Remove %s from cluster.\n", peer_name.c_str());
            peers[peer_name].send_enabled = false;
            peers[peer_name].receive_enabled = false;
            delete peers[peer_name].raft_message_client;
            peers[peer_name].raft_message_client = nullptr;
            peers.erase(peer_name);
        }
    }
    NodePeer & add_peer(const std::string & peer_name) {
        GUARD
        return add_peer(guard, peer_name);
    }
    void remove_peer(const std::string & peer_name){
        GUARD
        return remove_peer(guard, peer_name);
    }

    void do_apply() {

    }

    void switch_to(NodeState new_state) {
        NodeState o = state;
        state = new_state;
        invoke_callback(NUFT_CB_STATE_CHANGE, {this, o});
    }

    void send_heartbeat(std::lock_guard<std::mutex> & guard) {
        // TODO: optimize for every peer:
        // If already sent logs, don't do heartbeat this time.
        do_append_entries(guard, true);
    }

    void become_follower(TermID new_term) {
        if (state == NodeState::Leader) {
            // A splitted leader got re-connection,
            // Or step down, as required by the Raft Paper Chapter 6 Issue 2.
            // TODO: must stop heartbeat immediately
        } else {
            // Lose the election

        }
        switch_to(NodeState::Follower);
        current_term = new_term;
        vote_for = vote_for_none;
    }

    void become_leader() {
        switch_to(NodeState::Leader);
        vote_for = vote_for_none;
        leader_name = name;
        debug_node("Now I am the Leader of term %llu, because I got %llu votes.\n", current_term, vote_got);
        for (auto & pp : this->peers) {
            // Actually we don't know how many logs have been copied,
            // So we assume no log are copied to peer.
            pp.second.match_index = default_index_cursor;
            // "When a leader first comes to power,
            // it initializes all nextIndex values to the index just after the
            // last one in its log."
#define USE_COMMIT_INDEX
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
        debug_node("Lost Leader of (%s). Start election term: %lu, will timeout in %llu ms.\n", leader_name.c_str(), current_term, default_election_fail_timeout_interval);
        leader_name = "";
        // Same reason for setting `elect_timeout_due`. Ref. `reset_election_timeout`
        elect_timeout_due = UINT64_MAX;

        for (auto & pp : peers) {
            if(!pp.second.send_enabled) continue;
            raft_messages::RequestVoteRequest request;
            request.set_name(name);
            request.set_term(current_term);
            request.set_last_log_index(last_log_index());
            request.set_last_log_term(last_log_term());
            
            NodePeer & peer = pp.second;
            pp.second.voted_for_me = false;
            debug_node("Send RequestVoteRequest from %s to %s\n", name.c_str(), peer.name.c_str());
            assert(peer.raft_message_client != nullptr);
            peer.raft_message_client->AsyncRequestVote(request);
        }
    }

    int on_vote_request(raft_messages::RequestVoteResponse * response_ptr, const raft_messages::RequestVoteRequest & request) {
        // When receive a `RequestVoteRequest` from peer.

        std::string peer_name = request.name();
        if((!is_running()) || (!is_peer_receive_enabled(peer_name))){
        #if !defined(_HIDE_PAUSED_NODE_NOTICE)
            debug_node("Ignore RequestVoteRequest from %s, because (%s/%s) is not running.\n", request.name().c_str(), 
                    is_running()?"":"I", is_peer_receive_enabled(peer_name)?"":peer_name.c_str());
        #endif
            return -1;
        }
        GUARD
        
            
        raft_messages::RequestVoteResponse & response = *response_ptr;
        response.set_name(name);
        response.set_vote_granted(false);

        if (request.term() > current_term) {
            // An election is ongoing. 
            debug_node("Become Follower: Receive RequestVoteRequest from %s with new term of %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
            // We can't set `vote_for` to none here, or will cause multiple Leader elected(ref strange_failure3.log)
            // Error: vote_for = vote_for_none;
            // TODO An optimation from the Raft Paper Chapter 7 Issue 3: 
            // "removed servers can disrupt the cluster. These servers will not receive heartbeats, so they will time out and start new elections.
            // They will then send RequestVote RPCs with new term numbers, and this will cause the current leader to revert to follower state."
            become_follower(request.term());
            // We can't assume the sender has won the election.
            // So we can't set leader_name.
            reset_election_timeout();
            invoke_callback(NUFT_CB_ELECTION_END, {this, state==NodeState::Candidate?ELE_FAIL:ELE_SUC_OB});
        } else if (request.term() < current_term) {
            // Reject. A request of old term
            debug_node("Reject RequestVoteRequest because it is from a old term %llu, me %llu.\n", request.term(), current_term);
            goto end;
        }

        if (vote_for != vote_for_none && vote_for != request.name()) {
            // Reject. Already vote other
            debug_node("Reject RequestVoteRequest because I already vote for %s.\n", vote_for.c_str());
            goto end;
        }

        if (request.last_log_term() < last_log_term()) {
            debug_node("Reject RequestVoteRequest because request.last_log_term() < last_log_term(): %llu < %llu.\n", request.last_log_term(), last_log_term());
            goto end;
        }
        if (request.last_log_term() == last_log_term() && request.last_log_index() < last_log_index()) {
            debug_node("Reject RequestVoteRequest because request.last_log_index() < last_log_index(): %llu < %llu.\n", request.last_log_index(), last_log_index());
            goto end;
        }

        leader_name = "";
        // Vote.
        vote_for = request.name();
        response.set_vote_granted(true);
end:
        // Must set term here, cause `become_follower` may change it
        response.set_term(current_term);
        invoke_callback(NUFT_CB_ELECTION_START, {this, ELE_VOTE, response.vote_granted()});
        debug_node("Respond from %s to %s with %s\n", name.c_str(), request.name().c_str(), response.vote_granted() ? "YES" : "NO");
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

        if(peer.voting_rights){
            debug_node("Receive vote from %s = %s\n", response.name().c_str(), response.vote_granted() ? "YES" : "NO");
            if ((!peer.voted_for_me) && response.vote_granted()) {
                // If peer grant vote for me for the FIRST time.
                peer.voted_for_me = true;
                vote_got ++;
            }
            if (enough_votes(vote_got)) {
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
        }else{
            debug_node("Receive vote from %s, who has NO right for voting.\n", response.name().c_str());
        }
    }

    void do_append_entries(std::lock_guard<std::mutex> & guard, bool heartbeat) {
        // Send `AppendEntriesRequest` to all peer.
        // When heartbeat is set to true, this function will NOT send logs, even if there are some updates.
        // If you believe the logs are updated and need to be notified to peers, then you must set `heartbeat` to false.
        for (auto & pp : this->peers) {
            if(!pp.second.send_enabled) continue;
            NodePeer & peer = pp.second;
            // We copy log entries to peer, from `prev_log_index`.
            // This may fail when "an existing entry conflicts with a new one (same index but different terms)"
            IndexID prev = peer.next_index - 1;
            IndexID prev_log_index = prev;
            // TODO What if logs[prev] not exist?
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
// #if !defined(_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST)
                    if(heartbeat){}else
                    debug_node("Copy to peer %s LogEntries[%llu, %llu)\n", peer.name.c_str(), peer.next_index, logs.size());
// #endif
                }
                for (IndexID i = peer.next_index; i < logs.size(); i++) {
                    raft_messages::LogEntry & entry = *(request.add_entries());
                    entry = logs[i];
                }
            }

            #if defined(_HIDE_HEARTBEAT_NOTICE)
            if(heartbeat){}else
            #endif
            debug_node("Begin sending %s AppendEntriesRequest to %s, size %u.\n", heartbeat ? "heartbeat": "normal", peer.name.c_str(), request.entries_size());
            peer.raft_message_client->AsyncAppendEntries(request, heartbeat);
        }
    }

    int on_append_entries_request(raft_messages::AppendEntriesResponse * response_ptr, const raft_messages::AppendEntriesRequest & request) {
        // When a Follower/Candidate receive `AppendEntriesResponse` from Leader,
        // Try append entries, then return a `AppendEntriesResponse`.
        std::string peer_name = request.name();
        if((!is_running()) || (!is_peer_receive_enabled(peer_name))){
            #if !defined(_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST) && !defined(_HIDE_PAUSED_NODE_NOTICE)
            #if defined(_HIDE_HEARTBEAT_NOTICE)
            if(request.entries().size() == 0){}else
            #endif
            debug_node("Ignore AppendEntriesRequest from %s by %s, because {%s/%s} is not running.\n", 
                    request.name().c_str(), name.c_str(), paused?"I":"", 
                    is_peer_receive_enabled(peer_name)?"peer":"");
            #endif
            return -1;
        }
        GUARD
        raft_messages::AppendEntriesResponse & response = *response_ptr;
        response.set_name(name);
        response.set_success(false);

        IndexID prev_i = request.prev_log_index(), prev_j = 0;

        if (request.term() < current_term) {
            debug_node("Reject AppendEntriesRequest from %s, because it has older term %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
            goto end;
        }

        // **Current** Leader is still alive.
        // We can't reset_election_timeout by a out-dated AppendEntriesRPC.
        reset_election_timeout();

        if (request.term() > current_term) {
            // Discover a request with newer term.
            debug_node("Become Follower: Receive AppendEntriesRequest from %s with newer term %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
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
            debug_node("Become Follower: Receive AppendEntriesRequest from %s with term %llu. I lose election of my candidate term.\n", request.name().c_str(), request.term());
            become_follower(request.term());
            leader_name = request.name();
            invoke_callback(NUFT_CB_ELECTION_END, {this, ELE_FAIL});
        }


        // `request.prev_log_index() == -1` happens at the very beginning when a Leader with no Log, and send heartbeats to others.
        if (request.prev_log_index() >= 0 && request.prev_log_index() >= logs.size()) {
            // I don't have the `prev_log_index()` entry.
            debug_node("AppendEntries fail because I don't have prev_log_index = %llu, my log size = %llu.\n", request.prev_log_index(), logs.size());
            goto end;
        }
        if(request.prev_log_index() >= 0 && request.prev_log_index() + 1 < logs.size()){
            // I have some log entries(from former Leader), which current Leader don't have.
            debug_node("My logs.size-1 = %u > request.prev_log_index() = %lld. Maybe still probing, maybe condition 5.4.2(b)\n", logs.size()-1, request.prev_log_index());
            // logs.erase(logs.begin() + request.prev_log_index(), logs.end());
            // goto end;
        }
        if (request.prev_log_index() >= 0 && logs[request.prev_log_index()].term() != request.prev_log_term()) {
            // "If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it"
            assert(commit_index < request.prev_log_index());
            debug_node("AppendEntries fail because my term[prev_log_index=%lld] = %llu. Leader's prev_log_term= %llu, prev_log_index = %lld, commit = %lld. Do erase from Leader's prev_log_index to end.\n", 
                request.prev_log_index(), logs[request.prev_log_index()].term(), request.prev_log_term(), request.prev_log_index(), request.leader_commit());
            logs.erase(logs.begin() + request.prev_log_index(), logs.end());
            goto end;
        }

        prev_i++;
        for (; prev_i < logs.size() && prev_j < request.entries_size(); prev_i++, prev_j++) {
            // Remove all entries not meet with `request.entries()` after `prev_i`
            if (logs[prev_i].term() != request.entries(prev_j).term()) {
                assert(commit_index < prev_i);
                logs.erase(logs.begin() + prev_i, logs.end());
                break;
            }
        }
        if(request.entries_size()){
            debug_node("Leader %s wants to AppendEntries from %lld to %lld.\n", request.name().c_str(), logs.size(), logs.size() + request.entries_size() - prev_j);
        }
        if (prev_j < request.entries_size()) {
            // Copy the rest entries
            logs.insert(logs.end(), request.entries().begin() + prev_j, request.entries().end());
        }else{
            if(request.entries_size()){
                debug_node("Refuse to add any entries from Leader.\n");
            }
        }

        if (request.leader_commit() > commit_index) {
            // Update commit
            commit_index = std::min(request.leader_commit(), IndexID(logs.size()) - 1);
            debug_node("Leader %s ask me to advance commit_index to %lld.\n", request.name().c_str(), commit_index);
        }else{
            if(request.entries_size()){
                debug_node("Node %s commit_index remain %lld, because it's greater than %lld, entries_size = %u. Log size = %u, last_log_index = %lld\n", 
                        name.c_str(), commit_index, request.leader_commit(), request.entries_size(), logs.size(), last_log_index());
            }
        }

        // When received Configuration entry 
        for(IndexID i = IndexID(logs.size()) - 1; i >= std::max((IndexID)0, prev_i); i--){
            if(logs[i].command() == 1){
                on_update_configuration_joint(guard, logs[i]);
                break;
            } else if(logs[i].command() == 2){
                on_update_configuration_new(guard);
            }
        }
        // When Configuration entry is committed
        if(trans_conf){
            if(trans_conf->state == Configuration::State::OLD_JOINT && trans_conf->index <= commit_index){
                trans_conf->state == Configuration::State::JOINT;
                debug_node("Joint consensus committed by Leader.\n");
            } else if(trans_conf->state == Configuration::State::JOINT_NEW && trans_conf->index2 <= commit_index){
                on_update_configuration_finish(guard);
            }
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

        // debug_node("Receive response from %s\n", response.name().c_str());
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

        if (!response.success()) {
            // A failed `AppendEntriesRequest` can certainly not lead to `commit_index` updating
            debug("Peer %s returns AppendEntriesResponse: FAILED\n", response.name().c_str());
            return;
        }

        bool call_update_configuration_flag = false;
        bool committed_joint_flag = false;
        bool committed_new_flag = false;
        if(!peer.voting_rights){
            if(response.last_log_index() + 1 == logs.size()){
                // Now this node has catched up with me.
                debug_node("Node %s has now catched up with me, grant right for vote.\n", response.name().c_str());
                peer.voting_rights = true;
            }
            bool has_no_right = false;
            for(auto pr2: peers){
                if(!pr2.second.voting_rights){
                    has_no_right = true;
                    break;
                }
            }
            if(!has_no_right){
                call_update_configuration_flag = true;
            }
        }

        if(response.last_log_index() >= logs.size()){
            // This may possibly happen. ref the Raft Paper Chapter 5.4.2.
            // At phase(b) when S5 is elected and send heartbeat to S2.
            // NOTICE that in the Raft Paper, last_log_index and last_log_term are not sent by RPC.
            peer.next_index = logs.size();
            peer.match_index = peer.next_index - 1;
            debug_node("Peer %s's last_log_index %lld >= my log size = %u. My term %llu. Set next_index = %lld, match_index = %lld\n", 
                    response.name().c_str(), response.last_log_index(), logs.size(), current_term, peer.next_index, peer.match_index);
            // debug_node("Peer(%s). logs[response.last_log_index()].term() != response.last_log_term(). %lld != %lld. response.last_log_index() = %lld. logs.size = %u\n", 
            //        response.name().c_str(), logs[response.last_log_index()].term(), response.last_log_term(), response.last_log_index(), logs.size());
        }else{
            peer.next_index = response.last_log_index() + 1;
            peer.match_index = response.last_log_index();
        }
        
        // Until now peer.next_index is valid.
        IndexID new_commit = response.last_log_index();

        // Update commit
        if (commit_index >= response.last_log_index()) {
            // `commit_index` still ahead of peer's index.
            // This peer can't contribute to `commit_vote`
#if defined(_HIDE_HEARTBEAT_NOTICE)
            if(heartbeat){}else 
#endif
                debug_node("Peer(%s) grants NO commit vote. Peer's last_log_index(%lld) can't contribute to Leader's (%lld), there's NOTHING to commit.\n",
                    response.name().c_str(), response.last_log_index(), commit_index);
            goto CANT_COMMIT;
        }

        // TODO See `on_append_entries_request`
        assert(logs[response.last_log_index()].term() == response.last_log_term());
        if (current_term != logs[response.last_log_index()].term()) {
            // According to <<In Search of an Understandable Consensus Algorithm>>
            // "Raft never commits log entries from previous terms by counting replicas.
            // Only log entries from the leader’s current term are committed by counting replicas"

            // IMPORTANT consider the following situation:
            // 1. Follower B disconnect.
            // 2. Leader A add log entry X.
            // 3. Leader A lost its leadership by a re-connected Node B, when it starts a election.
            // 4. Node B will certainly lose the election, because it has obsolete entries.
            // 5. Node A become Leader again and replicate entries to B.
            // 6. Now B has exactly the same long as A, but A can't commit, because this rule.

// #if defined(_HIDE_HEARTBEAT_NOTICE)
            // if(!heartbeat) 
// #endif
                debug_node("Peer(%s) grants NO commit vote. commit_index(%lld) agrees. However term disagree(me:%llu, peer:%llu). \n", 
                        response.name().c_str(), commit_index, current_term, logs[response.last_log_index()].term());
            goto CANT_COMMIT;
        }
        
        // Now we can try to advance `commit_index` to `new_commit = response.last_log_index()`
        if(trans_conf && trans_conf->state == Configuration::State::OLD_JOINT && new_commit >= trans_conf->index){
            // Require majority of C_{old} and C_{new}
            size_t old_vote = 1;
            size_t new_vote = (trans_conf->is_in_new(name) ? 1 : 0);
            for(auto & pp: peers){
				assert(pp.second.voting_rights);
				if (pp.second.match_index >= new_commit) {
                    if(trans_conf->is_in_old(pp.second.name)){
                        old_vote++;
					}
                    if(trans_conf->is_in_new(pp.second.name)){
                        new_vote++;
                    }
                }
            }
            if(old_vote > trans_conf->oldvote_thres / 2 && new_vote > trans_conf->newvote_thres / 2){
			    debug_node("Advance commit_index from %lld to %lld. Joint consensus committed with support new %u(req %u), old %u(req %u)\n", 
                        commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
                commit_index = new_commit;
                trans_conf->state = Configuration::State::JOINT;
                // Some work must be done when we unlock the mutex at the end of the function. We mark here.
                committed_joint_flag = true;
            }else{
			    debug_node("CAN'T advance commit_index from %lld to %lld. Joint consensus CAN'T committed with support new %u(req %u), old %u(req %u)\n", 
                        commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
            }
        } else if(trans_conf && trans_conf->state == Configuration::State::JOINT_NEW && new_commit >= trans_conf->index2){
            // Require majority of C_{new}
            size_t new_vote = (trans_conf->is_in_new(name) ? 1 : 0);
            for(auto & pp: peers){
                assert(pp.second.voting_rights);
                if (pp.second.match_index >= new_commit) {
                    if(trans_conf->is_in_new(pp.second.name)){
                        new_vote++;
                    }
                }
            }
            if(new_vote > trans_conf->newvote_thres / 2){
                debug_node("Advance commit_index from %lld to %lld. New configuration committed with support new %u(req %u)\n", 
                        commit_index, new_commit, new_vote, trans_conf->newvote_thres);
                commit_index = new_commit;
                trans_conf->state = Configuration::State::NEW;
                committed_new_flag = true;
            }else{
                debug_node("CAN'T advance commit_index from %lld to %lld. New configuration CAN'T committed with support new %u(req %u)\n",
                        commit_index, new_commit, new_vote, trans_conf->newvote_thres);
            }
        }
        else{
            size_t commit_vote = 1; // This one is from myself.
            for (auto & pp : peers) {
                if (pp.second.voting_rights && pp.second.match_index >= new_commit) {
                    commit_vote++;
                }
            }
            // "A log entry is committed once the leader
            // that created the entry has replicated it on a majority of
            // the servers..."
            if (enough_votes(commit_vote)) {
                // if(!heartbeat) 
                    debug_node("Advance commit_index from %lld to %lld.\n", commit_index, new_commit);
                commit_index = new_commit;
                // "When the entry has been safely replicated, the leader applies the entry to its state machine 
                // and returns the result of that execution to the client."
                do_apply();
            }else{
                //if(!heartbeat) 
                    debug_node("Can't advance commit_index to %lld because of inadequate votes of %u.\n", new_commit, commit_vote);
            }
        }
        
CANT_COMMIT:
        if(call_update_configuration_flag){
            // Requires lock.
            update_configuration_joint(guard);
        }
        
        if(trans_conf){
            assert(trans_conf);

            if(committed_joint_flag){
                update_configuration_new(guard);
            }
            if(committed_new_flag){
                update_configuration_finish(guard);
            }
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
    
    NuftResult do_log(std::lock_guard<std::mutex> & guard, ::raft_messages::LogEntry entry, int command = 0){
        if(state != NodeState::Leader){
            // No, I am not the Leader, so please find the Leader first of all.
            debug("Not Leader!!!!!\n");
            return -NUFT_NOT_LEADER;
        }
        IndexID index = logs.size();
        entry.set_term(this->current_term);
        entry.set_index(index);
        entry.set_command(command);
        logs.push_back(entry);

        debug("Append LOCAL log Index %lld, Term %llu, commit_index %lld. Now copy to peers.\n", index, current_term, commit_index);
        // "The leader appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel..."
        do_append_entries(guard, false);
        return NUFT_OK;
    }
    bool do_log(std::lock_guard<std::mutex> & guard, const std::string & log_string){
        ::raft_messages::LogEntry entry;
        entry.set_data(log_string);
        return this->do_log(guard, entry);
    }
    // For API usage
    NuftResult do_log(::raft_messages::LogEntry entry, int command = 0){
        GUARD
        return do_log(guard, entry, command);
    }
    bool do_log(const std::string & log_string){
        GUARD
        return do_log(guard, log_string);
    }
    
    // API
    NuftResult update_configuration(const std::vector<std::string> & app, const std::vector<std::string> & rem){
        GUARD
        if(state != NodeState::Leader || paused){
            return -NUFT_NOT_LEADER;
        }
        if(trans_conf){
            // On the way.
            return -NUFT_FAIL;
        }
        trans_conf = new Configuration(app, rem, peers, name);
        trans_conf->state = Configuration::State::STAGING;
        debug_node("Update configuration with %u add, %u delete.\n", app.size(), rem.size());
        // Must come after we set up Configuration.
        for(auto & s: app){
            NodePeer & peer = add_peer(guard, s);
            peer.voting_rights = false;
        }
        if(app.size() > 0){
			// We don't send log immediately. 
			// Wait new nodes to catch up with the Leader.
            debug_node("New nodes added, switch to STAGING mode.\n");
        }else{
            update_configuration_joint(guard);
        }
        return NUFT_OK;
    }

    void update_configuration_joint(std::lock_guard<std::mutex> & guard){
        debug_node("Now all nodes catch up with me, starting switch to joint consensus.\n");
        // Now all nodes catched up and have voting rights.
        // Send our configuration to peer.
        ::raft_messages::LogEntry entry;
        trans_conf->index = logs.size();
        trans_conf->state = Configuration::State::OLD_JOINT;
        std::string conf_str = Configuration::to_string(*trans_conf);
        entry.set_data(conf_str);
        do_log(guard, entry, 1);
    }

    void on_update_configuration_joint(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry){
		// Followers received Leader's entry for joint consensus.
        std::string conf_str = entry.data();
        if(trans_conf){
            debug_node("Receive Leader's entry for joint consensus, but I'm already in state %d.\n", trans_conf->state);
            return;
        }
        trans_conf = new Configuration(Configuration::from_string(conf_str));
        trans_conf->state = Configuration::State::OLD_JOINT;
        debug_node("Receive Leader's entry for joint consensus. %u add %u delete. Request is %s\n", 
                trans_conf->app.size(), trans_conf->rem.size(), conf_str.c_str());
        for(auto & s: trans_conf->app){
            NodePeer & peer = add_peer(guard, s);
        }
    }

    void update_configuration_new(std::lock_guard<std::mutex> & guard){
        // After joint consensus is committed,
        // Leader can now start to switch to C_{new}.
        debug_node("Joint consensus committed.\n");
        assert(trans_conf);
        trans_conf->state = Configuration::State::JOINT;
        ::raft_messages::LogEntry entry;
        trans_conf->index2 = logs.size();
        entry.set_data("");
		do_log(guard, entry, 2); 
        trans_conf->state = Configuration::State::JOINT_NEW;
    }

    void on_update_configuration_new(std::lock_guard<std::mutex> & guard){
        debug_node("Receive Leader's entry for new configuration.\n");
        if(!trans_conf){
            debug_node("Receive Leader's entry for new configuration, but I'm not in update conf mode.\n");
            return;
        }
        if(trans_conf->state != Configuration::State::OLD_JOINT){
            debug_node("Receive Leader's entry for new configuration, but my state = %d.\n", trans_conf->state);
            return;
        }
        trans_conf->state = Configuration::State::JOINT_NEW;
        for(auto p: trans_conf->rem){
            debug_node("Remove peer %s\n", p.c_str());
            remove_peer(guard, p);
        }
    }

    void update_configuration_finish(std::lock_guard<std::mutex> & guard){
        // If the Leader is not in C_{new},
        // It should yield its Leadership and step down,
        // As soon as C_{new} is committed
        debug_node("Switch successfully to new consensus. Node state %d\n", this->state);
        for(auto p: trans_conf->rem){
            debug_node("Remove %s.\n", p.c_str());
            remove_peer(guard, p);
        }
        trans_conf->state = Configuration::State::NEW;
        invoke_callback(NUFT_CB_CONF_END, {this, this->state});
        // Make sure Leader leaves after new configuration committed.
        if(Nuke::contains(trans_conf->rem, name)){
            debug_node("Leader is not in C_{new}, step down.\n");
            become_follower(current_term);
            stop();
        }
        delete trans_conf;
        trans_conf = nullptr;
        debug_node("Leader finish updating config.\n");
    }

    void on_update_configuration_finish(std::lock_guard<std::mutex> & guard){
        if(!trans_conf){
            debug_node("Receive Leader's commit for new configuration, but I'm not in update conf mode.\n");
            return;
        }
        if(trans_conf->state != Configuration::State::JOINT_NEW){
            debug_node("Receive Leader's commit for new configuration, but my state = %d.\n", trans_conf->state);
            return;
        }
        trans_conf->state = Configuration::State::NEW;
        invoke_callback(NUFT_CB_CONF_END, {this, this->state});
        delete trans_conf;
        trans_conf = nullptr;
        debug_node("Follower finish updating config.\n");
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
                std::this_thread::sleep_for(30ms);
            }
        });
    }
    RaftNode(const RaftNode &) = delete;
    ~RaftNode() {
        debug("Destruct RaftNode %s.\n", name.c_str());
        GUARD
        tobe_destructed = true;
        state = NodeState::NotRunning;
        delete raft_message_server;
        timer_thread.join();
        for (auto & pp : this->peers) {
            NodePeer & peer = pp.second;
            delete (peer.raft_message_client);
        }
        if(trans_conf){
            delete trans_conf;
        }
    }

    void _list_peers(){
        for (auto & pp : this->peers) {
            printf("==> %s\n", pp.second.name.c_str());
        }
    }
};

RaftNode * make_raft_node(const std::string & addr);
