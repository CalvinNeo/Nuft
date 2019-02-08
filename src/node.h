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
#include <map>
#include <functional>
#include <algorithm>
#include "utils.h"
#include "grpc_utils.h"
#include "settings.h"
#include <tuple>

typedef std::string NodeName;
typedef int64_t IndexID;
typedef uint64_t TermID;

static constexpr IndexID default_index_cursor = -1; // When no log, index is -1
static constexpr TermID default_term_cursor = 0; // When starting, term is 0
static constexpr uint64_t default_timeout_interval_lowerbound = 150 + 300; // 1000;
static constexpr uint64_t default_timeout_interval_upperbound = 300 + 300; // 1800;
static constexpr uint64_t default_heartbeat_interval = 30;
// the Raft paper: Each candidate
// restarts its randomized election timeout at the start of an
// election, and it waits for that timeout to elapse before
// starting the next election; 
// This implementation does not conform to the standard.
static constexpr uint64_t default_election_fail_timeout_interval = 550; // 3000; 
#define vote_for_none ""

#define GUARD std::lock_guard<std::mutex> guard((mut));
#if defined(USE_GRPC_ASYNC)
// Use Async gRPC model
typedef RaftMessagesClientAsync RaftMessagesClient;
#else
// Use Sync gRPC model
#if defined(USE_GRPC_STREAM)
typedef RaftMessagesStreamClientSync RaftMessagesClient;
#else
typedef RaftMessagesClientSync RaftMessagesClient;
#endif
#define GRPC_SYNC_CONCUR_LEVEL 8 
#endif

struct Persister{
    struct RaftNode * node = nullptr;
    void Dump(std::lock_guard<std::mutex> &, bool backup_conf = false);
    void Load(std::lock_guard<std::mutex> &);
};

struct NodePeer {
    NodeName name;
    bool voted_for_me = false;
    // Index of next log to copy
    IndexID next_index = default_index_cursor + 1;
    // Index of logs already copied
    IndexID match_index = default_index_cursor;
    // Use `RaftMessagesClient` to send RPC message to peer.
    std::shared_ptr<RaftMessagesClient> raft_message_client;
    // Debug usage
    bool receive_enabled = true;
    bool send_enabled = true;
    // According to the Raft Paper Chapter 6 Issue 1,
    // Newly added nodes is in staging mode, and thet have no voting rights,
    // Until they are sync-ed.
    bool voting_rights = true;
    // seq nr for next rpc call
    uint64_t seq = 0;
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
    NUFT_CB_ON_NEW_ENTRY,
    NUFT_CB_SIZE,
};
enum NUFT_CMD_TYPE {
    NUFT_CMD_NORMAL = 0,
    NUFT_CMD_TRANS = 1,
    NUFT_CMD_SNAPSHOT = 2,
    NUFT_CMD_TRANS_NEW = 3,
    NUFT_CMD_SIZE,
};
struct NuftCallbackArg{
    struct RaftNode * node;
    std::lock_guard<std::mutex> * lk = nullptr;
    int a1 = 0;
    int a2 = 0;
    void * p1 = nullptr;
};
typedef int NuftResult;
// typedef NuftResult NuftCallbackFunc(NUFT_CB_TYPE, NuftCallbackArg *);
// typedef std::function<NuftResult(NUFT_CB_TYPE, NuftCallbackArg *)> NuftCallbackFunc;
typedef std::function<NuftResult(NUFT_CB_TYPE, NuftCallbackArg *)> NuftCallbackFunc;

struct ApplyMessage{
    IndexID index = default_index_cursor;
    TermID term = default_term_cursor;
    std::string name;
    bool from_snapshot = false;
    std::string data;
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

    static const char * node_state_name(NodeState s) {
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
        void init(){
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
        Configuration(const std::vector<std::string> & a, const std::vector<std::string> & r, const std::vector<NodeName> & o) : app(a), rem(r), old(o){
            init();
        }
        Configuration(const std::vector<std::string> & a, const std::vector<std::string> & r, const std::map<NodeName, NodePeer*> & o, const NodeName & leader_exclusive) : app(a), rem(r){
            for(auto && p: o){
                old.push_back(p.first);
            }
            // Include myself
            old.push_back(leader_exclusive);
            init();
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
            // No cluster membership changes.
            // This state is used when we initialize a node, especially from a persist storage.
            BLANK = 0,
            // In staging period, no entries are logged.
            STAGING = 1,
            // Now entry for joint consensus is logged, but not committed.
            OLD_JOINT = 2,
            // Now entry for joint consensus is committed.
            JOINT = 3,
            // Now entry for new configuration are sent.
            // ref update_configuration_new
            JOINT_NEW = 4,
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
    };

    // Standard
    TermID current_term = default_term_cursor;
    NodeName vote_for = vote_for_none;
    std::vector<raft_messages::LogEntry> logs;
    std::string leader_name;
    struct Configuration * trans_conf = nullptr;
    struct Persister * persister = nullptr; // If set to nulltr then persist is not enabled.

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
    size_t new_vote = 0;
    size_t old_vote = 0;

    // Network
    std::map<NodeName, NodePeer*> peers;
    std::vector<NodePeer*> tobe_removed_peers;
    #if defined(USE_GRPC_STREAM)
    RaftStreamServerContext * raft_message_server = nullptr;
    #else
    RaftServerContext * raft_message_server = nullptr;
    #endif
    std::thread timer_thread;
    uint64_t start_timepoint; // Reject RPC from previous test cases.
    // Ref. elaborate_strange_erase.log
    uint64_t last_timepoint; // TODO Reject RPC from previous meeage from current Leader.
    uint64_t last_seq;

    // RPC utils for sync
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_STREAM) && !defined(USE_GRPC_SYNC_BARE) 
    // All peer's RaftMessagesClient share one `sync_client_task_queue`.
    // std::shared_ptr<Nuke::ThreadExecutor> sync_client_task_queue;
    Nuke::ThreadExecutor * sync_client_task_queue = nullptr;
#endif

    // Utils
    // Multiple thread can access the same object, use mutex for protection.
    mutable std::mutex mut;

    // API
    NuftCallbackFunc callbacks[NUFT_CB_SIZE];
    bool debugging = false;

    void print_state(std::lock_guard<std::mutex> & guard){
        printf("%15s %12s %5s %9s %7s %7s %6s %6s\n",
                "Name", "State", "Term", "log size", "commit", "peers", "run", "trans");
        printf("%15s %12s %5llu %9u %7lld %7u %6s %6d\n", name.c_str(),
                RaftNode::node_state_name(state), current_term,
                logs.size(), commit_index, peers.size(),
                is_running(guard)? "T" : "F", (!trans_conf) ? 0 : trans_conf->state);
    }

    NuftResult invoke_callback(NUFT_CB_TYPE type, NuftCallbackArg arg){
        if(callbacks[type]){
            // If is callable
            return (callbacks[type])(type, &arg);
        }
        return NUFT_OK;
    }

    void set_callback(std::lock_guard<std::mutex> & guard, NUFT_CB_TYPE type, NuftCallbackFunc func){
        callbacks[type] = func;
    }
    void set_callback(NUFT_CB_TYPE type, NuftCallbackFunc func){
        GUARD
        set_callback(guard, type, func);
    }

    IndexID get_base_index() const{
        if(logs.size() == 0){
            return default_index_cursor;
        }
        return logs[0].index();    
    }
    IndexID last_log_index() const {
        if(logs.size() == 0){
            return default_index_cursor;
        }
        // This assertion is tested to be true.
        // However it will fail when log is compacted.
        if(get_base_index() == 0){
            assert(logs.size() == logs.back().index() + 1);
        }
        return logs.back().index();
    }
    IndexID last_log_term() const {
        return logs.size() == 0 ? default_term_cursor : logs.back().term();
    }

    ::raft_messages::LogEntry & gl(IndexID index){
        // Find logs[i].index() == logs[i].index()
        IndexID base = get_base_index();
        if(index - base >= 0){
            return logs[index - base];
        } else{
            // TODO Retrieve from persisted store.
            debug_node("Access index %lld\n", index);
            assert(false);
        }
    }
    size_t find_entry(IndexID index, TermID term){
        for(int i = logs.size() - 1; i >= 0; i--){
            if(logs[i].term() == term && logs[i].index() == index){
                return i;
            }
        }
        return default_index_cursor;
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
            if(pr.second->voting_rights){
                voters ++;
            }
        }
        return voters;
    }
    
    template <typename F>
    int enough_votes_trans_conf(F f){
        // "Once a given server adds the new configuration entry to its log,
        // it uses that configuration for all future decisions (a server
        // always uses the latest configuration in its log, regardless
        // of whether the entry is committed). "
        // NOTICE C_{new} can make decisions sorely in `JOINT_NEW` stage when we decide whether to advance to NEW stage.
        // "Again, this configuration will take effect on each server as soon as it is seen.
        // When the new configuration has been committed under
        // the rules of C_{new}"
        old_vote = 0; new_vote = 0;
        if(trans_conf && Nuke::in(trans_conf->state, {Configuration::State::OLD_JOINT, Configuration::State::JOINT})){
            // Require majority of C_{old} and C_{new}
            old_vote = 1;
            new_vote = (trans_conf->is_in_new(name) ? 1 : 0);
            for(auto & pp: peers){
                NodePeer & peer = *pp.second;
                assert(peer.voting_rights);
                if (f(peer)) {
                    // If got votes from peer
                    if(trans_conf->is_in_old(peer.name)){
                        old_vote++;
                    }
                    if(trans_conf->is_in_new(peer.name)){
                        new_vote++;
                    }
                }
            }
            assert(trans_conf->oldvote_thres > 0);
            assert(trans_conf->newvote_thres > 0);
            if(old_vote > trans_conf->oldvote_thres / 2 && new_vote > trans_conf->newvote_thres / 2){
                return 1;
            }else{
                return -1;
            }
        } 
        else if(trans_conf && Nuke::in(trans_conf->state, {Configuration::State::NEW, Configuration::State::JOINT_NEW})){
            // Require majority of C_{new}
            new_vote = (trans_conf->is_in_new(name) ? 1 : 0);
            for(auto & pp: peers){
                NodePeer & peer = *pp.second;
                assert(peer.voting_rights);
                if (f(peer)) {
                    if(trans_conf->is_in_new(peer.name)){
                        new_vote++;
                    }
                }
            }
            if(new_vote > trans_conf->newvote_thres / 2){
                return 1;
            }else{
                return -1;
            }
        }
        return 0;
    }
    
    NodeName get_leader_name() const{
        // This function helps Clients to locate Leader.
        // Keep in mind that in raft, Clients only communicate with Leader.
        if(state == NodeState::Leader){
            return name;
        }
        // There's no Leader.
        return leader_name;
    }
    bool test_election();
    void on_timer() {
        // Check heartbeat
        uint64_t current_ms = get_current_ms();

        GUARD
        if (!is_running(guard)) return;
        if ((state == NodeState::Leader) && current_ms >= default_heartbeat_interval + last_tick) {
            // Leader's routine heartbeat
            send_heartbeat(guard);
            last_tick = current_ms;
        }
        // Check if leader is timeout
        if ((state == NodeState::Follower || state == NodeState::Candidate)
                && (current_ms >= elect_timeout_due)
                ) { 
            // Start election(from a Follower), or restart a election(from a Candidate)
            do_election(guard);
            // In `do_election`, `leader_name` is set to "".
            if(state == NodeState::Follower){
                invoke_callback(NUFT_CB_ELECTION_START, {this, &guard, ELE_NEW});
            }else{
                invoke_callback(NUFT_CB_ELECTION_START, {this, &guard, ELE_AGA});
            }
        }

        // Check if election is timeout
        if ((state == NodeState::Candidate)
                && (current_ms >= election_fail_timeout_due)
                ) {
            // In this case, no consensus is reached during the previous election,
            // We restart a new election.
            if (test_election()) {
                // Maybe there is only me in the cluster...
                become_leader(guard);
                invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_SUC});
            } else {
                debug_node("Election failed(timeout) with %u votes at term %lu.\n", vote_got, current_term);
                reset_election_timeout();
                invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_T});
            }
        }

        if(trans_conf && trans_conf->state == Configuration::State::JOINT && commit_index >= trans_conf->index){
            update_configuration_new(guard);
        }
    }

    void run(std::lock_guard<std::mutex> & guard) {
        debug_node("Run node. Switch state to Follower.\n");
        if (state != NodeState::NotRunning) {
            return;
        }
        paused = false;
        state = NodeState::Follower;
        persister->Dump(guard, true);
        reset_election_timeout();
    }
    void run(){
        GUARD
        run(guard);
    }
    void run(std::lock_guard<std::mutex> & guard, const std::string & new_name){
        name = new_name;
        persister->Load(guard);
        run(guard);
    }
    void run(const std::string & new_name){
        GUARD
        run(guard, new_name);
    }

    void apply_conf(std::lock_guard<std::mutex> & guard, const Configuration & conf);
    void apply_conf(const Configuration & conf);

    void stop_unguarded() {
        paused = true;
        debug_node("Stop node of %s\n", name.c_str());
    }
    void stop() {
        GUARD
        stop(guard);
    }
    void stop(std::lock_guard<std::mutex> & guard) {
        stop_unguarded();
    }
    void resume(std::lock_guard<std::mutex> & guard) {
        paused = false;
        debug_node("Resume node of %s\n", name.c_str());
    }
    bool is_running_unguard() const{
        return (state != NodeState::NotRunning) && (!paused) 
            && (!tobe_destructed); // Prevent Ghost RPC called by `RaftMessagesServiceImpl::RequestVote` etc
    }
    bool is_running(std::lock_guard<std::mutex> & guard) const{
        return (state != NodeState::NotRunning) && (!paused) 
            && (!tobe_destructed); // Prevent Ghost RPC called by `RaftMessagesServiceImpl::RequestVote` etc
    }
    bool is_running() const{
        GUARD
        return is_running(guard);
    }

    void enable_receive(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers.at(peer_name)->receive_enabled = true;
        }
    }
    void disable_receive(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers.at(peer_name)->receive_enabled = false;
        }
    }
    void enable_send(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers.at(peer_name)->send_enabled = true;
        }
    }
    void disable_send(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        if(Nuke::contains(peers, peer_name)){
            peers.at(peer_name)->send_enabled = false;
        }
    }
    bool is_peer_receive_enabled(std::lock_guard<std::mutex> & guard, const std::string & peer_name) const{
        if(!Nuke::contains(peers, peer_name)) return false;
        return peers.at(peer_name)->receive_enabled;
    }
    bool is_peer_send_enabled(std::lock_guard<std::mutex> & guard, const std::string & peer_name) const{
        if(!Nuke::contains(peers, peer_name)) return false;
        return peers.at(peer_name)->send_enabled;
    }

    void enable_receive(const std::string & peer_name){
        GUARD
        enable_receive(guard, peer_name);
    }
    void disable_receive(const std::string & peer_name){
        GUARD
        disable_receive(guard, peer_name);
    }
    void enable_send(const std::string & peer_name){
        GUARD
        enable_send(guard, peer_name);
    }
    void disable_send(const std::string & peer_name){
        GUARD
        disable_send(guard, peer_name);
    }
    bool is_peer_receive_enabled(const std::string & peer_name) const{
        GUARD
        return is_peer_receive_enabled(guard, peer_name);
    }
    bool is_peer_send_enabled(const std::string & peer_name) const{
        GUARD
        return is_peer_send_enabled(guard, peer_name);
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

    NodePeer * add_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name);
    NodePeer * add_peer(const std::string & peer_name);
    void remove_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name);
    void remove_peer(const std::string & peer_name);
    void reset_peers(std::lock_guard<std::mutex> & guard);
    void reset_peers();
    void clear_removed_peers(std::lock_guard<std::mutex> & guard);
    void clear_removed_peers();
    void wait_clients_shutdown();
    
    void do_apply(bool from_snapshot = false) {
        GUARD
        do_apply(guard, from_snapshot);
    }
    void do_apply(std::lock_guard<std::mutex> & guard, bool from_snapshot = false) {
        assert(!(from_snapshot && gl(last_applied + 1).command() != NUFT_CMD_SNAPSHOT));
        debug_node("Do apply from %lld to %lld\n", last_applied + 1, commit_index);
        for(IndexID i = last_applied + 1; i <= commit_index; i++){
            ApplyMessage * applymsg = new ApplyMessage{i, gl(i).term(), name, from_snapshot, gl(i).data()};
            NuftResult res = invoke_callback(NUFT_CB_ON_APPLY, {this, &guard, 0, 0, applymsg});
            if(res != NUFT_OK){
                debug_node("Apply fail at %lld\n", i);
            }else{
                last_applied = i;
            }
            delete applymsg;
        }
        debug_node("Do apply end.\n");
    }

    void send_heartbeat(std::lock_guard<std::mutex> & guard);
    void switch_to(std::lock_guard<std::mutex> & guard, NodeState new_state);
    void become_follower(std::lock_guard<std::mutex> & guard, TermID new_term);
    void become_leader(std::lock_guard<std::mutex> & guard);

    void truncate_log(IndexID last_included_index, TermID last_included_term);
    NuftResult do_install_snapshot(std::lock_guard<std::mutex> & guard, IndexID last_included_index, const std::string & state_machine_state);
    NuftResult do_install_snapshot(IndexID last_included_index, const std::string & state_machine_state);
    void do_send_install_snapshot(std::lock_guard<std::mutex> & guard, NodePeer & peer);
    int on_install_snapshot_request(raft_messages::InstallSnapshotResponse* response_ptr, const raft_messages::InstallSnapshotRequest& request);
    void on_install_snapshot_response(const raft_messages::InstallSnapshotResponse & response);

    void do_election(std::lock_guard<std::mutex> & guard);
    int on_vote_request(raft_messages::RequestVoteResponse * response_ptr, const raft_messages::RequestVoteRequest & request);
    void on_vote_response(const raft_messages::RequestVoteResponse & response);


    template <typename R>
    bool handle_request_routine(std::lock_guard<std::mutex> & guard, const R & request){

        if (request.term() < current_term) {
            debug_node("Reject AppendEntriesRequest/InstallSnapshotRequest from %s, because it has older term %llu, me %llu. It maybe a out-dated Leader\n", request.name().c_str(), request.term(), current_term);
            return false;
        }

        // **Current** Leader is still alive.
        // We can't reset_election_timeout by a out-dated AppendEntriesRPC.
        reset_election_timeout();

        if (request.term() > current_term) {
            // Discover a request with newer term.
            print_state(guard);
            debug_node("Become Follower: Receive AppendEntriesRequest/InstallSnapshotRequest from %s with newer term %llu, me %llu, my time %llu, peer time %llu.\n", 
                request.name().c_str(), request.term(), current_term, (uint64_t)start_timepoint % 10000, (uint64_t)request.time() % 10000);
            become_follower(guard, request.term());
            leader_name = request.name();
            // Observe the emergence of a new Leader.
            invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_SUC_OB});
        }
        if (request.term() == current_term && state == NodeState::Follower && leader_name == ""){
            // In this case, this node is a Follower, maybe through `RequestVoteRequest`.
            // Now new leader is elected and not knowing the elected leader.
            // This function is mostly for debugging/API, in order to trigger callback, otherwise the debugging thread may block forever at `cv.wait()`
            // IMPORTANT: We can't call `become_follower` here because it will reset vote_for.
            // Consider the following situation(total 3 nodes):
            // 1. A ask for B's vote, B granted, A become leader.
            // 2. A don't have enough time to send AppendEntriesRequest
            // 3. C timeout without acknowledging A's leadership
            // 4. C ask for A's vote
            // 5. If we call become_follower, B will forgot it has already voted for A
            // Error: become_follower(request.term());
            leader_name = request.name();
            invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_SUC_OB});
        }
        if (request.term() == current_term && state == NodeState::Candidate) {
            // In election and a Leader has won. 
            debug_node("Become Follower: Receive AppendEntriesRequest/InstallSnapshotRequest from %s with term %llu. I lose election of my candidate term.\n", request.name().c_str(), request.term());
            become_follower(guard, request.term());
            leader_name = request.name();
            invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_FAIL});
        }
        return true;
    }
    std::string print_logs(){
        std::string r;
        for(int i = 0; i < logs.size(); i++){
            r += std::to_string(logs[i].index()) + "(" + std::to_string(logs[i].term()) + ");";
        }
        return r;
    }

    void do_append_entries(std::lock_guard<std::mutex> & guard, bool heartbeat);
    int on_append_entries_request(raft_messages::AppendEntriesResponse * response_ptr, const raft_messages::AppendEntriesRequest & request);
    void on_append_entries_response(const raft_messages::AppendEntriesResponse & response, bool heartbeat);

    NuftResult get_log(IndexID index, ::raft_messages::LogEntry & l) {
        GUARD
        if(index <= last_log_index()){
            l = gl(index);
            return NUFT_OK;
        }
        return -NUFT_FAIL;
    }
    
    // NuftResult do_log(std::lock_guard<std::mutex> & guard, ::raft_messages::LogEntry entry, int command = 0){
    //     auto x = [](RaftNode *){};
    //     return do_log(guard, entry, x, command);
    // }
    // NuftResult do_log(std::lock_guard<std::mutex> & guard, const std::string & log_string){
    //     auto x = [](RaftNode *){};
    //     return do_log(guard, log_string, x);
    // }
    template <typename F>
    NuftResult do_log(std::lock_guard<std::mutex> & guard, ::raft_messages::LogEntry entry, F f, int command){
        if(state != NodeState::Leader){
            // No, I am not the Leader, so please find the Leader first of all.
            debug_node("Not Leader!!!!!\n");
            return -NUFT_NOT_LEADER;
        }
        if(!is_running(guard)){
            debug_node("Not Running!!!!\n");
            return -NUFT_FAIL;
        }
        IndexID index = last_log_index() + 1;
        entry.set_term(this->current_term);
        entry.set_index(index);
        entry.set_command(command);
        logs.push_back(entry);
        f(this);
        if(persister) persister->Dump(guard);
        
        debug_node("Append LOCAL log Index %lld, Term %llu, commit_index %lld. Now copy to peers.\n", index, current_term, commit_index);
        // "The leader appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel..."
        do_append_entries(guard, false);
        return index;
    }
    template <typename F>
    NuftResult do_log(std::lock_guard<std::mutex> & guard, const std::string & log_string, F f){
        ::raft_messages::LogEntry entry;
        entry.set_data(log_string);
        return do_log(guard, entry, f, NUFT_CMD_NORMAL);
    }
    // For API usage
    NuftResult do_log(::raft_messages::LogEntry entry, int command){
        GUARD
        return do_log(guard, entry, [](RaftNode *){}, command);
    }
    NuftResult do_log(const std::string & log_string){
        GUARD
        return do_log(guard, log_string, [](RaftNode *){});
    }
    template <typename F>
    NuftResult do_log(::raft_messages::LogEntry entry, F f, int command){
        GUARD
        return do_log(guard, entry, f, command);
    }
    template <typename F>
    NuftResult do_log(const std::string & log_string, F f){
        GUARD
        return do_log(guard, log_string, f);
    }
    
    // API
    NuftResult update_configuration(const std::vector<std::string> & app, const std::vector<std::string> & rem);
    void update_configuration_joint(std::lock_guard<std::mutex> & guard);
    void on_update_configuration_joint(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry);
    void on_update_configuration_joint_committed(std::lock_guard<std::mutex> & guard);
    void update_configuration_new(std::lock_guard<std::mutex> & guard);
    void on_update_configuration_new(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry);
    void update_configuration_finish(std::lock_guard<std::mutex> & guard);
    void on_update_configuration_finish(std::lock_guard<std::mutex> & guard);

    void safe_leave(std::lock_guard<std::mutex> & guard){
        // Remove Leader safely when new conf is committed on peer.
        // Remove Peer safely 
        if(state == NodeState::Leader){
            debug_node("Leader is not in C_{new}, step down and stop.\n");
        }else{
            debug_node("I am not in C_{new}, stop.\n");
        }
        become_follower(guard, current_term);
        stop(guard);
    }

    void safe_leave(){
        GUARD
        safe_leave(guard);
    }
    template<typename T>
    void set_seq_nr(NodePeer & peer, T & request){
        if(peer.seq == 0){
            request.set_initial(true);
        }else{
            request.set_initial(false);
        }
        peer.seq++;
        request.set_seq(peer.seq);
    }
    bool valid_seq(uint64_t seq, bool initial){
        if(initial){
            last_seq = seq;
            return true;
        }else if(seq > last_seq){
            last_seq = seq;
            return true;
        }
        return false;
    }

    RaftNode(const std::string & addr) : state(NodeState::NotRunning), name(addr) {
        current_term = default_term_cursor;
        vote_for = vote_for_none;
        leader_name = "";
        trans_conf = nullptr;
        persister = new Persister{this};
        commit_index = default_index_cursor;
        last_applied = default_index_cursor;
        paused = false;
        tobe_destructed = false;
        last_tick = 0; 
        elect_timeout_due = 0; 
        election_fail_timeout_due = 0; 
        vote_got = 0;
        new_vote = 0;
        old_vote = 0;
        // NOTICE In a streaming implementation, I need to first listen to some port, 
        // then create a client
        #if defined(USE_GRPC_STREAM)
        raft_message_server = new RaftStreamServerContext(this);
        #else
        raft_message_server = new RaftServerContext(this);
        #endif
        using namespace std::chrono_literals;
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_STREAM) && !defined(USE_GRPC_SYNC_BARE)
        // sync_client_task_queue = std::make_shared<Nuke::ThreadExecutor>(GRPC_SYNC_CONCUR_LEVEL);
        sync_client_task_queue = new Nuke::ThreadExecutor(GRPC_SYNC_CONCUR_LEVEL);
#endif
        start_timepoint = get_current_ms();
        last_seq = 0;
        timer_thread = std::thread([&]() {
            while (1) {
                if(tobe_destructed){
                    return;
                }
                this->on_timer();
                std::this_thread::sleep_for(std::chrono::duration<int, std::milli> {default_heartbeat_interval});
            }
        });
    }
    RaftNode(const RaftNode &) = delete;
    ~RaftNode() {
        debug_node("Destruct RaftNode.\n");
        {
            GUARD
            tobe_destructed = true;
            paused = true;
            state = NodeState::NotRunning;
            debug_node("Reset Peers \n");
            reset_peers(guard);
            debug_node("Delete Persister \n");
            delete persister;
        }
        {
            // NOTICE We must first release mut, then acquire it.
            // See `void RaftMessagesStreamClientSync::handle_response():t2` deadlock
            debug_node("Clear Deleted Peers \n");
            // wait_clients_shutdown();
            clear_removed_peers();
        }

        {
            // NOTICE See stream.clear.timeout.log
            // Seems a deadlock may happend when join
            debug_node("Wait timer_thread join\n");
            timer_thread.join();
            debug_node("timer_thread Joined\n");
        }
        {
            debug_node("Delete Server\n");
            // This may block forever even we called shutdown.
            delete raft_message_server;
            raft_message_server = nullptr;
            debug_node("Delete Server End\n");
        }
        {
            GUARD
            debug_node("Delete Conf\n");
            if(trans_conf){
                delete trans_conf;
            }
        }
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_STREAM) && !defined(USE_GRPC_SYNC_BARE)
        // TODO this will sometimes blocks at thread pool.
        // I think it may because of:
        // 1. ~RaftNode locks mut
        // 2. delete `sync_client_task_queue` will wait until all pending RPCs are handled
        // 3. one response arrived, e.g. call `on_append_entries_response`
        // 4. it will try to acquire mut again
        // 5. deadlock
        delete sync_client_task_queue;
#endif
        debug_node("Destruct finish\n");
    }
};

RaftNode * make_raft_node(const std::string & addr);
