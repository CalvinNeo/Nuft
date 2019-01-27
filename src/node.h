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

#define debug_node(...) NUKE_LOG(LOGLEVEL_DEBUG, RaftNodeLogger{this}, ##__VA_ARGS__)
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
typedef RaftMessagesClientSync RaftMessagesClient;
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
    RaftServerContext * raft_message_server = nullptr;
    std::thread timer_thread;
    uint64_t start_timepoint; // Reject RPC from previous test cases.
    // Ref. elaborate_strange_erase.log
    uint64_t last_timepoint; // TODO Reject RPC from previous meeage from current Leader.

    // RPC utils for sync
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
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
    bool test_election(){
        if (trans_conf){
            int ans = enough_votes_trans_conf([&](const NodePeer & peer){return peer.voted_for_me;});
            if(ans > 0) {
                goto VOTE_ENOUGH;
            }else if(ans < 0) {
                goto VOTE_NOT_ENOUGH;
            }else{
                goto NORMAL_TEST_VOTE;
            }
        } 
NORMAL_TEST_VOTE:
        vote_got = 1;
        for(auto & pp: peers){
            const NodePeer & peer = *(pp.second);
            if(peer.voted_for_me){
                vote_got ++;
            }
        }
        if (!enough_votes(vote_got)) {
            goto VOTE_NOT_ENOUGH;
        }
VOTE_ENOUGH:
        return true;
VOTE_NOT_ENOUGH:
        return false;
    }
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
        raft_message_server = new RaftServerContext(this);
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
    void apply_conf(std::lock_guard<std::mutex> & guard, const Configuration & conf){
        debug_node("Apply conf: old %u, app %u, rem %u\n", conf.old.size(), conf.app.size(), conf.rem.size());
        for(int i = 0; i < conf.old.size(); i++){
            if(conf.old[i] != name && !(conf.state >= Configuration::State::JOINT_NEW && Nuke::contains(conf.rem, conf.old[i]))){
                add_peer(guard, conf.old[i]);
            } else{
                debug_node("Apply % fail. conf.state %d\n", conf.old[i].c_str(), conf.state);
            }
        }
        for(int i = 0; i < conf.app.size(); i++){
            if(conf.app[i] != name && (conf.state >= Configuration::State::OLD_JOINT)){
                add_peer(guard, conf.app[i]);
            }
        }
        debug_node("Apply conf end.\n");
    }
    void apply_conf(const Configuration & conf){
        GUARD
        apply_conf(guard, conf);
    }

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

    NodePeer * add_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name) {
        //TODO Bad idea to return this reference, use `unique_ptr` later
        assert(peer_name != "");
        assert(peer_name != name);
        if (Nuke::contains(peers, peer_name)) {
            debug_node("Peer %s already found. Now size %u\n", peer_name.c_str(), peers.size());
        } else {
            debug_node("Add Peer %s. Now size %u\n", peer_name.c_str(), peers.size());
            peers[peer_name] = new NodePeer{peer_name};
            peers[peer_name]->send_enabled = true;
            peers[peer_name]->receive_enabled = true;
            peers[peer_name]->raft_message_client = std::make_shared<RaftMessagesClient>(peer_name, this);
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
            peers[peer_name]->raft_message_client->task_queue = sync_client_task_queue;
#endif
        }
        return peers[peer_name];
    }
    void remove_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
        // Must under the protection of mutex
        if(Nuke::contains(peers, peer_name)){
            debug_node("Remove %s from this->peers.\n", peer_name.c_str());
            peers[peer_name]->send_enabled = false;
            peers[peer_name]->receive_enabled = false;
            // delete peers[peer_name]->raft_message_client;
            // peers[peer_name]->raft_message_client = nullptr;

            // NOTICE There is a log in addpeer.core.log that shows there is a data race between here
            // and `RaftMessagesClientSync::AsyncAppendEntries`'s `!strongThis->raft_node` cheking.
            // So we just comment this stmt, because `receive_enabled` can help us do so.
            // peers[peer_name]->raft_message_client->raft_node = nullptr;

            delete peers[peer_name];
            peers[peer_name] = nullptr;
            peers.erase(peer_name);
        }
    }
    NodePeer * add_peer(const std::string & peer_name) {
        GUARD
        return add_peer(guard, peer_name);
    }
    void remove_peer(const std::string & peer_name){
        GUARD
        return remove_peer(guard, peer_name);
    }
    void reset_peers(std::lock_guard<std::mutex> & guard){
        for (auto & pp : this->peers) {
            remove_peer(guard, pp.first);
        }
    }
    void reset_peers(){
        GUARD
        reset_peers(guard);
    }

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

    void switch_to(std::lock_guard<std::mutex> & guard, NodeState new_state) {
        NodeState o = state;
        state = new_state;
        invoke_callback(NUFT_CB_STATE_CHANGE, {this, &guard, o});
    }

    void send_heartbeat(std::lock_guard<std::mutex> & guard) {
        // TODO: optimize for every peer:
        // If already sent logs, don't do heartbeat this time.
        do_append_entries(guard, true);
    }

    void become_follower(std::lock_guard<std::mutex> & guard, TermID new_term) {
        if (state == NodeState::Leader) {
            // A splitted leader got re-connection,
            // Or step down, as required by the Raft Paper Chapter 6 Issue 2.
            // TODO: must stop heartbeat immediately
        } else {
            // Lose the election

        }
        switch_to(guard, NodeState::Follower);
        current_term = new_term;
        vote_for = vote_for_none;
    }

    void become_leader(std::lock_guard<std::mutex> & guard) {
        switch_to(guard, NodeState::Leader);
        // IMPORTANT: We should not reset vote_for.
        // Error: vote_for = vote_for_none;
        leader_name = name;
        if(trans_conf){
            debug_node("Now I am the Leader of term %llu, because I got vote new %u thres %u old %u thres %u.\n", 
                current_term, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
        }else{
            debug_node("Now I am the Leader of term %llu, because I got %llu votes.\n", current_term, vote_got);
        }
        for (auto & pp : this->peers) {
            // Actually we don't know how many logs have been copied,
            // So we assume no log are copied to peer.
            NodePeer & peer = *pp.second;
            peer.match_index = default_index_cursor;
            // "When a leader first comes to power,
            // it initializes all nextIndex values to the index just after the
            // last one in its log."
#define USE_COMMIT_INDEX
#if !defined(USE_COMMIT_INDEX)
            peer.next_index = last_log_index() + 1;
            assert(peer.next_index >= 0);
#else
            // This strategy is from Mushroom's implementation.
            peer.next_index = commit_index + 1;
#endif
        }

        // Heartbeat my leadership
        // Do not call `do_append_entries` or cause deadlock.
        last_tick = 0;
    }

    void truncate_log(IndexID last_included_index, TermID last_included_term){
        // TODO Can move some element from `logs` to `new_entries`
        std::vector<::raft_messages::LogEntry> new_entries;
        ::raft_messages::LogEntry entry;
        entry.set_term(last_included_term);
        entry.set_index(last_included_index);
        entry.set_command(NUFT_CMD_SNAPSHOT); // This is a snapshot
        new_entries.push_back(entry);
        size_t i = find_entry(last_included_index, last_included_term);
        if (i != -1 && i + 1 < logs.size()) {
            new_entries.insert(new_entries.end(), logs.begin() + i + 1, logs.end());
        }
        logs = new_entries;
    }

    NuftResult do_install_snapshot(std::lock_guard<std::mutex> & guard, IndexID last_included_index, const std::string & state_machine_state){
        if(last_log_index() <= get_base_index()){
            debug_node("Already snapshotted.\n");
            return NUFT_FAIL;
        }
        if(last_log_index() > last_log_index()){
            debug_node("Error index.\n");
            return NUFT_FAIL;
        }
        if(last_log_index() > last_applied){
            debug_node("We can only snapshot until last_applied.\n");
            return NUFT_FAIL;
        }

        // Create snapshot, truncate log, persist
        TermID last_included_term = gl(last_included_index).term();
        // (last_included_index, last_included_term) should be 1st in new `logs`.
        logs.erase(logs.begin(), logs.begin() + last_included_index - get_base_index());
        logs[0].set_index(last_included_index);
        logs[0].set_term(last_included_term);
        logs[0].set_command(NUFT_CMD_SNAPSHOT);
        logs[0].set_data(state_machine_state);

        if(persister) persister->Dump(guard);
    }
    NuftResult do_install_snapshot(IndexID last_included_index, const std::string & state_machine_state){
        GUARD
        do_install_snapshot(last_included_index, state_machine_state);
    }


    void do_send_install_snapshot(std::lock_guard<std::mutex> & guard, const NodePeer & peer){
        // NOTICE We don't loop over all peers because we want to patch this function into `do_append_entries`
        if(!peer.send_enabled) return;
        if(peer.next_index > get_base_index()) {
            return;
        }
        raft_messages::InstallSnapshotRequest request;
        IndexID base = get_base_index();
        if(gl(base).command() != NUFT_CMD_SNAPSHOT){
            debug_node("gl(%lld).command() != SNAPSHOT", base);
            exit(-1);
        }
        request.set_name(name);
        request.set_term(current_term);
        request.set_last_included_index(base);
        request.set_last_included_term(base==default_index_cursor? default_term_cursor: gl(base).term());
        request.set_data(gl(base).data()); // TODO get snapshot from log entry (term:last_included_term, index:last_included_index)
        request.set_time(get_current_ms());
        
        debug_node("Send InstallSnapshotRequest from %s to %s\n", name.c_str(), peer.name.c_str());
        peer.raft_message_client->AsyncInstallSnapshot(request);
    }

    int on_install_snapshot_request(raft_messages::InstallSnapshotResponse* response_ptr, const raft_messages::InstallSnapshotRequest& request) {
        GUARD

        const std::string & peer_name = request.name();
        if((!is_running(guard)) || (!is_peer_receive_enabled(guard, peer_name))){
        #if !defined(_HIDE_PAUSED_NODE_NOTICE)
            debug_node("Ignore InstallSnapshotRequest from %s, I state %d paused %d, Peer running %d.\n", request.name().c_str(), 
                    state, paused, is_peer_receive_enabled(guard, peer_name));
        #endif
            return -1;
        }
        if((uint64_t)request.time() < (uint64_t)start_timepoint){
            return 0;
        }
        raft_messages::InstallSnapshotResponse & response = *response_ptr;
        response.set_name(name);
        response.set_success(false);

        if(handle_request_routine(guard, request)){
            response.set_term(current_term);
        }else{
            debug_node("Leader ask me to do snapshot, but term disagree.\n");
            goto end;
        }

        // Create snapshot, truncate log, persist
        // NOTICE We must save a copy of `get_base_index()`, because it will be modified.
        debug_node("Leader ask me to do snapshot.\n");
        truncate_log(request.last_included_index(), request.last_included_term());
        logs[0].set_command(NUFT_CMD_SNAPSHOT);
        logs[0].set_data(request.data());
        // Below two asserts DO NOT STAND.
        // assert(last_applied < request.last_included_index());
        // assert(commit_index < request.last_included_index());
        commit_index = request.last_included_index();
        last_applied = commit_index - 1; // Force apply the snapshot
        do_apply(guard, 1);

        persister->Dump(guard);
        response.set_success(true);

end:
        IndexID base = get_base_index();
        response.set_term(current_term);
        response.set_last_included_index(base);
        response.set_last_included_term(base==default_index_cursor? default_term_cursor: gl(base).term());
        response.set_time(get_current_ms());
        return 0;
    }

    void on_install_snapshot_response(const raft_messages::InstallSnapshotResponse & response) {
        GUARD

        if((uint64_t)response.time() < (uint64_t)start_timepoint){
            return;
        }
        if (state != NodeState::Leader) {
            return;
        }
        if (!response.success()) {
            debug_node("Peer %s returns InstallSnapshotResponse: FAILED\n", response.name().c_str());
            return;
        }
        if (response.term() > current_term) {
            become_follower(guard, response.term());
            return;
        }

        if(!Nuke::contains(peers, response.name())){
            return;
        }
        NodePeer & peer = *(peers[response.name()]);
        peer.next_index = response.last_included_index() + 1;
        peer.match_index = response.last_included_index();
    }

    void do_election(std::lock_guard<std::mutex> & guard) {
        
        switch_to(guard, NodeState::Candidate);
        current_term++;
        // NOTICE Now compute `vote_for` in `test_election` by counting `vote_for_me`
        // vote_got = 1; // Vote for myself
        vote_for = name;
        if(persister) persister->Dump(guard);

        uint64_t current_ms = get_current_ms();
        election_fail_timeout_due = current_ms + default_election_fail_timeout_interval;
        debug_node("Lost Leader of (%s). Start election term: %lu, will timeout in %llu ms. My last_log_index = %lld\n", 
            leader_name.c_str(), current_term, default_election_fail_timeout_interval, last_log_index());
        leader_name = "";
        // Same reason for setting `elect_timeout_due`. Ref. `reset_election_timeout`
        elect_timeout_due = UINT64_MAX;

        for (auto & pp : peers) {
            if(!pp.second->send_enabled) continue;
            raft_messages::RequestVoteRequest request;
            request.set_name(name);
            request.set_term(current_term);
            request.set_last_log_index(last_log_index());
            request.set_last_log_term(last_log_term());
            request.set_time(get_current_ms());
            
            NodePeer & peer = *pp.second;
            peer.voted_for_me = false;
            debug_node("Send RequestVoteRequest from %s to %s\n", name.c_str(), peer.name.c_str());
            // assert(peer.raft_message_client != nullptr);
            peer.raft_message_client->AsyncRequestVote(request);
        }
    }

    int on_vote_request(raft_messages::RequestVoteResponse * response_ptr, const raft_messages::RequestVoteRequest & request) {
        // When receive a `RequestVoteRequest` from peer.

        GUARD
        std::string peer_name = request.name();
        // Log file super_delayed_election.log shows that we need to guard `is_peer_receive_enabled`
        if((!is_running(guard)) || (!is_peer_receive_enabled(guard, peer_name))){
        #if !defined(_HIDE_PAUSED_NODE_NOTICE)
            debug_node("Ignore RequestVoteRequest from %s, My state %d paused %d, Peer receive_enabled %d.\n", request.name().c_str(), 
                    state, paused, is_peer_receive_enabled(guard, peer_name));
        #endif
            return -1;
        }
        if((uint64_t)request.time() < (uint64_t)start_timepoint){
            return -1;
        }
            
        raft_messages::RequestVoteResponse & response = *response_ptr;
        response.set_name(name);
        response.set_vote_granted(false);

        if (request.term() > current_term) {
            // An election is ongoing. 
            debug_node("Become Follower: Receive RequestVoteRequest from %s with new term of %llu, me %llu. My last_log_index %lld, peer %lld\n", 
                request.name().c_str(), request.term(), current_term, last_log_index(), last_log_term());
            // NOTICE We can't set `vote_for` to none here, or will cause multiple Leader elected(ref strange_failure3.log)
            // TODO An optimation from the Raft Paper Chapter 7 Issue 3: 
            // "removed servers can disrupt the cluster. These servers will not receive heartbeats, so they will time out and start new elections.
            // They will then send RequestVote RPCs with new term numbers, and this will cause the current leader to revert to follower state."
            become_follower(guard, request.term());
            // We can't assume the sender has won the election, actually we don't know who wins, maybe no one has won yet.
            // So we can't set leader_name to request.name().
            leader_name = "";
            reset_election_timeout();
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
            debug_node("Reject RequestVoteRequest from %s because request.last_log_term() < last_log_term(): %llu < %llu.\n", 
                request.name().c_str(), request.last_log_term(), last_log_term());
            goto end;
        }
        if (request.last_log_term() == last_log_term() && request.last_log_index() < last_log_index()) {
            debug_node("Reject RequestVoteRequest from %s because request.last_log_index() < last_log_index(): %llu < %llu.\n", 
                request.name().c_str(), request.last_log_index(), last_log_index());
            goto end;
        }

        leader_name = "";
        // Vote.
        vote_for = request.name();
        response.set_vote_granted(true);
end:
        // Must set term here, cause `become_follower` may change it
        response.set_term(current_term);
        response.set_time(get_current_ms());
        invoke_callback(NUFT_CB_ELECTION_START, {this, &guard, ELE_VOTE, response.vote_granted()});
        debug_node("Respond from %s to %s with %s\n", name.c_str(), request.name().c_str(), response.vote_granted() ? "YES" : "NO");
        if(persister) persister->Dump(guard);
        return 0;
    }

    void on_vote_response(const raft_messages::RequestVoteResponse & response) {
        // Called from server.h
        // When a candidate receives vote from others

        if((uint64_t)response.time() < (uint64_t)start_timepoint){
            return;
        }
        GUARD
        if (state != NodeState::Candidate) {
            return;
        }

        if(!Nuke::contains(peers, response.name())){
            // debug("Receive vote from unregisted %s = %s\n", response.name().c_str(), response.vote_granted() ? "YES" : "NO");
            return;
        }
        // `operator[]` will create automaticlly
        NodePeer & peer = *(peers[response.name()]);

        if(peer.voting_rights){
            debug_node("Receive vote from %s = %s\n", response.name().c_str(), response.vote_granted() ? "YES" : "NO");
            if (response.term() > current_term) {
                // Found newer term from response. My election definitely fail.
                // However, we don't know who's the leader yet, and the Leader may not exist now,
                // because this may be new election.
                become_follower(guard, response.term());
                if(persister) persister->Dump(guard);
                invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_FAIL});
            }else{
                if ((!peer.voted_for_me) && response.vote_granted()) {
                    // If peer grant vote for me for the FIRST time.
                    peer.voted_for_me = true;
                    // vote_got ++;
                }
                if(test_election()){
                    assert(response.term() <= current_term);
                    become_leader(guard);
                    invoke_callback(NUFT_CB_ELECTION_END, {this, &guard, ELE_SUC});
                }
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
            NodePeer & peer = *pp.second;
            if(!peer.send_enabled) continue;
            if(peer.next_index <= get_base_index() && get_base_index() > 0){
                // If lag behind too much
                do_send_install_snapshot(guard, peer);
            }else{
                // We copy log entries to peer, from `prev_log_index`.
                // This may fail when "an existing entry conflicts with a new one (same index but different terms)"
                IndexID prev_log_index = peer.next_index - 1;
                // What if entry prev_log_index not exist? 
                // We add rules in `on_append_entries_response`, now peer.next_index can't be set GT last_log_index() + 1
                TermID prev_log_term = prev_log_index > default_index_cursor ? gl(prev_log_index).term() : default_term_cursor;
                raft_messages::AppendEntriesRequest request;
                request.set_name(name);
                request.set_term(current_term);
                request.set_prev_log_index(prev_log_index);
                request.set_prev_log_term(prev_log_term);
                request.set_leader_commit(commit_index);
                request.set_time(get_current_ms());
                // Add entries to request
                // NOTICE Even this is a heartbeat RPC, we still need to check entries.
                // Because some nodes may suffer from network failure etc., 
                // and fail to update their log when we firstly sent entries in `do_log`.
                if(peer.next_index < last_log_index() + 1){
                    if(heartbeat){}else
                    debug_node("Copy to peer %s LogEntries[%lld, %lld]\n", peer.name.c_str(), peer.next_index, last_log_index());
                }else{
                    if(heartbeat){}else
                    debug_node("Peer %s next_index = %lld, me last %lld, No Append.\n", peer.name.c_str(), peer.next_index, last_log_index());
                }
                // TODO Handle when logs is compacted
                assert(peer.next_index >= get_base_index());
                for (IndexID i = std::max((IndexID)0, peer.next_index); i <= last_log_index() ; i++) {
                    raft_messages::LogEntry & entry = *(request.add_entries());
                    entry = gl(i);
                }

                #if defined(_HIDE_HEARTBEAT_NOTICE)
                if(heartbeat){}else
                #endif
                debug_node("Send %s AppendEntriesRequest to %s, size %u term %llu.\n", 
                        heartbeat ? "heartbeat": "normal", peer.name.c_str(), request.entries_size(),
                        current_term);
                peer.raft_message_client->AsyncAppendEntries(request, heartbeat);
            }
        }
    }

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

    int on_append_entries_request(raft_messages::AppendEntriesResponse * response_ptr, const raft_messages::AppendEntriesRequest & request) {
        // When a Follower/Candidate receive `AppendEntriesResponse` from Leader,
        // Try append entries, then return a `AppendEntriesResponse`.

        // In a earlier version, we require lock after we check `is_running()`, this may lead to segfault. See coredump_at_contains.log
        GUARD

        std::string peer_name = request.name();
        if((!is_running(guard)) || (!is_peer_receive_enabled(guard, peer_name))){
            #if !defined(_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST) && !defined(_HIDE_PAUSED_NODE_NOTICE)
            #if defined(_HIDE_HEARTBEAT_NOTICE)
            if(request.entries().size() == 0){}else
            #endif
            debug_node("Ignore AppendEntriesRequest from %s, I state %d paused %d, Peer running %d.\n", request.name().c_str(), 
                    state, paused, is_peer_receive_enabled(guard, peer_name));
            #endif
            return -1;
        }
        if((uint64_t)request.time() < (uint64_t)start_timepoint){
            debug_node("Out-dated AppendEntriesRequest\n");
            return -1;
        }
        if(!Nuke::contains(peers, peer_name)){
            // NOTICE Must double check here.
            // Otherwise, a deferred `on_append_entries_request` may not have seen that
            // we removed Leader. See wierd_erase.log
            debug_node("Ignore AppendEntriesRequest from %s. I don't have this peer.\n", request.name().c_str());
            return -1;
        }
        raft_messages::AppendEntriesResponse & response = *response_ptr;
        response.set_name(name);
        response.set_success(false);

        IndexID prev_i = request.prev_log_index(), prev_j = 0;

        if(handle_request_routine(guard, request)){
            response.set_term(current_term);
        }else{
            // When `handle_request_routine` fails, it prints error messages.
            goto end;
        }

        // `request.prev_log_index() == -1` happens at the very beginning when a Leader with no Log, and send heartbeats to others.
        if (request.prev_log_index() >= 0 && request.prev_log_index() > last_log_index()) {
            // I don't have the `prev_log_index()` entry.
            debug_node("AppendEntries fail. I don't have prev_log_index = %lld, my last_log_index = %lld.\n", request.prev_log_index(), last_log_index());
            goto end;
        }
        if(request.prev_log_index() >= 0 && request.prev_log_index() < last_log_index()){
            // I have some log entries(from former Leader), which current Leader don't have.
            // Can erase them only we conflicts happen.
            // debug_node("My last_log_index = %u > request.prev_log_index() = %lld. Maybe still probing, maybe condition 5.4.2(b)\n", last_log_index(), request.prev_log_index());
        }
        // "If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it"
        // TODO IMPORTANT When I convert this into MIT 6.824 tests, It will fail TestFigure8Unreliable2C

        if(request.prev_log_index() == default_index_cursor){
            // If Leader has no entry, then we definitely not match.
            goto DO_ERASE;
        }
        if(request.prev_log_index() > get_base_index()){
            TermID wrong_term = gl(request.prev_log_index()).term();
            if(request.prev_log_term() != wrong_term){
                for(IndexID i = request.prev_log_index() - 1; i >= default_index_cursor; i--){
                    if(i == default_index_cursor){
                        debug_node("Revoke last_log_index to %lld, remove all wrong entries of term %llu.\n", default_index_cursor, wrong_term);
                        response.set_last_log_index(default_index_cursor);
                        response.set_last_log_term(default_term_cursor);
                    } else if(gl(i).term() != wrong_term){
                        // Now we found non conflict entries.
                        debug_node("Revoke last_log_index to %lld, remove all wrong entries of term %llu.\n", i, wrong_term);
                        response.set_last_log_index(i);
                        response.set_last_log_term(gl(i).term());
                    }
                    debug_node("Revoke last_log_index finished.");
                    goto end2;
                }
            }
        }
DO_ERASE:
        if(request.prev_log_index() >= get_base_index()){
            // Reserve logs range (, request.prev_log_index()]
            if(request.prev_log_index() + 1 <= last_log_index()){
                // NOTICE There is a situation where `on_update_configuration_joint` is called IMMEDIATELY before Erase. 
                // This is because I mixed up some `NUFT_CMD_*`s in some early version.
                debug_node("Erase [%lld, ). Before, last_log_index %lld, on Leader's side, prev_log_index %lld. logs = %s\n", 
                    request.prev_log_index() + 1, last_log_index(), request.prev_log_index(), print_logs().c_str());
                logs.erase(logs.begin() + request.prev_log_index() + 1 - get_base_index(), logs.end());
                debug_node("Insert size %u\n", request.entries_size());
            }
            logs.insert(logs.end(), request.entries().begin(), request.entries().end());
        }

        if (request.leader_commit() > commit_index) {
            // Update commit
            // Seems we can't make this assertion here,
            // according to Figure2 Receiver Implementation 5.
            // Necessary, see https://thesquareplanet.com/blog/students-guide-to-raft/
            // assert(request.leader_commit() <= last_log_index());
            commit_index = std::min(request.leader_commit(), last_log_index());
            debug_node("Leader %s ask me to advance commit_index to %lld.\n", request.name().c_str(), commit_index);
            do_apply(guard);
        }else{
            if(request.entries_size()){
                debug_node("My commit_index remain %lld, because it's GE than Leader's commit %lld, entries_size = %u. last_log_index = %lld\n", 
                    commit_index, request.leader_commit(), request.entries_size(), last_log_index());
            }
        }

        // std::atomic_signal_fence(std::memory_order_acq_rel);
        // When received Configuration entry 
        for(IndexID i = last_log_index(); i >= std::max((IndexID)0, prev_i) && i >= get_base_index(); i--){
            if(gl(i).command() == NUFT_CMD_TRANS){
                on_update_configuration_joint(guard, gl(i));
                break;
            } else if(gl(i).command() == NUFT_CMD_TRANS_NEW){
                on_update_configuration_new(guard, gl(i));
            }
        }
        // When Configuration entry is committed
        if(trans_conf){
            if(trans_conf->state == Configuration::State::OLD_JOINT && trans_conf->index <= commit_index){
                trans_conf->state == Configuration::State::JOINT;
                persister->Dump(guard);
                debug_node("Advance index to %lld >= trans_conf->index %lld, Joint consensus committed.\n", commit_index, trans_conf->index);
            } else if(trans_conf->state == Configuration::State::JOINT_NEW && trans_conf->index2 <= commit_index){
                on_update_configuration_finish(guard);
            }
        }
succeed:
        response.set_success(true);
end:
        // Must set term here, because `become_follower` may change it
        response.set_term(current_term);
        response.set_last_log_index(last_log_index());
        response.set_last_log_term(last_log_term());
        response.set_time(get_current_ms());
end2:
        if(persister) persister->Dump(guard);
        return 0;
    }

    void on_append_entries_response(const raft_messages::AppendEntriesResponse & response, bool heartbeat) {
        // When Leader receive `AppendEntriesResponse` from others
        
        GUARD
        
        IndexID new_commit;
        if((uint64_t)response.time() < (uint64_t)start_timepoint){
            return;
        }
        // debug_node("Receive response from %s\n", response.name().c_str());
        if (state != NodeState::Leader) {
            return;
        }

        if (response.term() > current_term) {
            // I know I am not the Leader by now.
            // However, I don't know who is the Leader yet.
            // This is most likely a recovering from network partitioning.
            // ref `on_vote_response`.
            debug_node("Become Follower because find newer term %llu from Peer %s.\n", response.term(), response.name().c_str());
            become_follower(guard, response.term());
            if(persister) persister->Dump(guard);
        } else if (response.term() != current_term) {
            // Reject. Invalid term
            return;
        }        
        if(!Nuke::contains(peers, response.name())){
            // From a removed peer(by conf trans)
            return;
        }

        NodePeer & peer = *(peers[response.name()]);

        if (!response.success()) {
            // A failed `AppendEntriesRequest` can certainly not lead to `commit_index` updating
            debug_node("Peer %s returns AppendEntriesResponse: FAILED\n", response.name().c_str());
            peer.next_index = response.last_log_index() + 1;
            peer.match_index = response.last_log_index();
            return;
        }

        // NOW WE SEE IF WE CAN COMMIT
        bool finish_staging_flag = false;
        bool committed_joint_flag = false;
        bool committed_new_flag = false;
        if(!peer.voting_rights){
            if(response.last_log_index() == last_log_index()){
                // Now this node has catched up with me.
                debug_node("Node %s has now catched up with me, grant right for vote.\n", response.name().c_str());
                peer.voting_rights = true;
            }
            // If there are no staging nodes, staging finished.
            if(std::find_if(peers.begin(), peers.end(), [](auto & pp){
                NodePeer & peer2 = *pp.second;
                return peer2.voting_rights == false;
            }) == peers.end()){
                finish_staging_flag = true;
            }
        }

        if(response.last_log_index() > last_log_index()){
            // This may possibly happen. ref the Raft Paper Chapter 5.4.2.
            // At phase(b) when S5 is elected and send heartbeat to S2.
            // NOTICE that in the Raft Paper, last_log_index and last_log_term are not sent by RPC.
            peer.next_index = last_log_index() + 1;
            peer.match_index = last_log_index();
            goto CANT_COMMIT;
        }else{
            peer.next_index = response.last_log_index() + 1;
            peer.match_index = response.last_log_index();
        }
        
        // Until now peer.next_index is valid.
        new_commit = response.last_log_index();

        // Update commit
        if (commit_index >= new_commit) {
            // `commit_index` still ahead of peer's index.
            // This peer can't contribute to `commit_vote`
#if defined(_HIDE_HEARTBEAT_NOTICE)
            if(heartbeat){}else 
#endif
                debug_node("Peer(%s) grants NO commit vote. Peer's last_log_index(%lld) can't contribute to Leader's (%lld), there's NOTHING to commit.\n",
                    response.name().c_str(), response.last_log_index(), commit_index);
            goto CANT_COMMIT;
        }

        if(gl(response.last_log_index()).term() != response.last_log_term()){
            // This may possibly happen. see the Raft Paper Chapter 5.4.2.
            debug_node("Peer(%s) have conflict log at last index. My (index %lld, term %llu), Peer (index %lld, term %llu) = %lld.\n",
                    response.name().c_str(), response.last_log_index(), gl(response.last_log_index()).term(), response.last_log_index(), response.last_log_term());
        }
        if (current_term != gl(response.last_log_index()).term()) {
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

            // if(!heartbeat) 
                debug_node("Peer(%s) can't commit logs replicated by previous Leaders. Term disagree at commit_index(%lld), (me:%llu, peer:%llu). \n", 
                        response.name().c_str(), commit_index, current_term, gl(response.last_log_index()).term());
            goto CANT_COMMIT;
        }
        
        // Now we can try to advance `commit_index` to `new_commit = response.last_log_index()` by voting.
        if(trans_conf){
            int ans = enough_votes_trans_conf([&](const NodePeer & peer){
                return peer.match_index >= new_commit;
            });
            if(ans > 0){
                if(trans_conf && trans_conf->state == Configuration::State::JOINT_NEW && new_commit >= trans_conf->index2){
                    debug_node("Advance commit_index from %lld to %lld. New configuration committed with support new %u(req %u)\n", 
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres);
                    commit_index = new_commit;
                    trans_conf->state = Configuration::State::NEW;
                    committed_new_flag = true;
                }else if (trans_conf->state == Configuration::State::OLD_JOINT && new_commit >= trans_conf->index){
                    debug_node("Advance commit_index from %lld to %lld. Joint consensus committed with support new %u(req %u), old %u(req %u)\n", 
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
                    commit_index = new_commit;
                    trans_conf->state = Configuration::State::JOINT;
                    // Some work must be done when we unlock the mutex at the end of the function. We mark here.
                    committed_joint_flag = true;
                }else{
                    commit_index = new_commit;
                    debug_node("Advance commit_index from %lld to %lld. With support new %u(req %u), old %u(req %u)\n", 
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
                }
            }else if(ans < 0){
                if(trans_conf && trans_conf->state == Configuration::State::JOINT_NEW && new_commit >= trans_conf->index2){
                    debug_node("CAN'T advance commit_index from %lld to %lld. New configuration CAN'T committed with support new %u(req %u)\n",
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres);
                }else if (trans_conf->state == Configuration::State::OLD_JOINT && new_commit >= trans_conf->index){
                    debug_node("CAN'T advance commit_index from %lld to %lld. Joint consensus CAN'T committed with support new %u(req %u), old %u(req %u)\n", 
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
                }else{
                    debug_node("CAN'T advance commit_index from %lld to %lld. With support new %u(req %u), old %u(req %u)\n", 
                            commit_index, new_commit, new_vote, trans_conf->newvote_thres, old_vote, trans_conf->oldvote_thres);
                }
            }else{
                goto NORMAL_TEST_COMMIT;
            }
        }
        else{
NORMAL_TEST_COMMIT:
            size_t commit_vote = 1; // This one is from myself.
            std::string mstr;
            for (auto & pp : peers) {
                NodePeer & peer2 = *pp.second;
                if (peer2.voting_rights && peer2.match_index >= new_commit) {
                    mstr += (peer2.name + ":" + std::to_string(peer2.match_index) + ";");
                    commit_vote++;
                }
            }
            // "A log entry is committed once the leader
            // that created the entry has replicated it on a majority of
            // the servers..."
            if (enough_votes(commit_vote)) {
                // if(!heartbeat) 
                    debug_node("Advance commit_index from %lld to %lld with vote %d. match_index {%s}.\n", commit_index, new_commit, commit_vote, mstr.c_str());
                commit_index = new_commit;
                // "When the entry has been safely replicated, the leader applies the entry to its state machine 
                // and returns the result of that execution to the client."
                do_apply(guard);
            }else{
                //if(!heartbeat) 
                    debug_node("Can't advance commit_index to %lld because of inadequate votes of %u.\n", new_commit, commit_vote);
            }
        }
        
CANT_COMMIT:
        if(finish_staging_flag){
            // Requires lock.
            update_configuration_joint(guard);
        }
        
        if(trans_conf){
            if(committed_joint_flag){
                on_update_configuration_joint_committed(guard);
            }
            if(committed_new_flag){
                update_configuration_finish(guard);
            }
        }
    }

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
            if(s != name){
                NodePeer & peer = *add_peer(guard, s);
                peer.voting_rights = false;
            }
        }
        if(app.size() > 0){
            // We don't send log immediately. 
            // Wait new nodes to catch up with the Leader.
            debug_node("New nodes added, switch to STAGING mode.\n");
        }else{
            update_configuration_joint(guard);
        }
        persister->Dump(guard);
        return NUFT_OK;
    }

    void update_configuration_joint(std::lock_guard<std::mutex> & guard){
        debug_node("Now all nodes catch up with me, starting switch to joint consensus.\n");
        invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::OLD_JOINT});
        // Now all nodes catched up and have voting rights.
        // Send our configuration to peer.
        ::raft_messages::LogEntry entry;
        trans_conf->index = last_log_index() + 1;
        trans_conf->state = Configuration::State::OLD_JOINT;
        std::string conf_str = Configuration::to_string(*trans_conf);
        entry.set_data(conf_str);
        do_log(guard, entry, [](RaftNode *){}, 1);
        persister->Dump(guard);
    }

    void on_update_configuration_joint(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry){
        // Followers received Leader's entry for joint consensus.
        std::string conf_str = entry.data();
        if(trans_conf){
            // debug_node("Receive Leader's entry for joint consensus, but I'm already in state %d.\n", trans_conf->state);
            return;
        }
        trans_conf = new Configuration(Configuration::from_string(conf_str));
        trans_conf->state = Configuration::State::OLD_JOINT;
        trans_conf->index = entry.index();
        debug_node("Receive Leader's entry for joint consensus. %u add %u delete. Request is %s. oldvote_thres = %u, newvote_thres = %u\n", 
                trans_conf->app.size(), trans_conf->rem.size(), conf_str.c_str(), trans_conf->oldvote_thres, trans_conf->newvote_thres);
        for(auto & s: trans_conf->app){
            if(s != name){
                NodePeer & peer = *add_peer(guard, s);
            }
        }
        persister->Dump(guard);
    }

    void on_update_configuration_joint_committed(std::lock_guard<std::mutex> & guard){
        // After joint consensus is committed,
        // Leader can now start to switch to C_{new}.
        debug_node("Joint consensus committed.\n");
        assert(trans_conf);
        trans_conf->state = Configuration::State::JOINT;
        persister->Dump(guard);
        invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::JOINT});
    }

    void update_configuration_new(std::lock_guard<std::mutex> & guard){
        // TODO If we crash at JOINT, then no Leader will issue entry for new configuration.
        // Need split here
        ::raft_messages::LogEntry entry;
        trans_conf->index2 = last_log_index() + 1;
        entry.set_data("");
        do_log(guard, entry, [](RaftNode *){}, NUFT_CMD_TRANS_NEW); 
        trans_conf->state = Configuration::State::JOINT_NEW;
        persister->Dump(guard);
        invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::JOINT_NEW});
    }

    void on_update_configuration_new(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry){
        if(!trans_conf){
            debug_node("Receive Leader's entry for new configuration, but I'm not in update conf mode.\n");
            return;
        }
        if(trans_conf->state != Configuration::State::OLD_JOINT){
            debug_node("Receive Leader's entry for new configuration, but my state = %d.\n", trans_conf->state);
            return;
        }
        // debug_node("Receive Leader's entry for new configuration.\n");
        trans_conf->state = Configuration::State::JOINT_NEW;
        trans_conf->index2 = entry.index();
        for(auto p: trans_conf->rem){
            // NOTICE When we remove Leader, we can no longer receive message from Leader, this will lead to a election.
            // We can't remove Leader when:
            // "In this case, the leader steps
            //  down (returns to follower state) once it has committed the
            //  Cnew log entry. This means that there will be a period of
            //  time (while it is committingCnew) when the leader is managing a cluster that does not include itself; it replicates log
            //  entries but does not count itself in majorities."
            if(p == leader_name){
                debug_node("Can't remove peer %s NOW because it is Leader.\n", p.c_str());
            }else{
                debug_node("Remove peer %s.\n", p.c_str());
                remove_peer(guard, p);
            }
        }
        persister->Dump(guard);
    }

    void update_configuration_finish(std::lock_guard<std::mutex> & guard){
        // If the Leader is not in C_{new},
        // It should yield its Leadership and step down,
        // As soon as C_{new} is committed.
        debug_node("Leader Switch successfully to new consensus. From state %d\n", this->state);
        for(auto p: trans_conf->rem){
            debug_node("Remove %s.\n", p.c_str());
            remove_peer(guard, p);
        }
        trans_conf->state = Configuration::State::NEW;
        // Make sure Leader leaves after new configuration committed.
        invoke_callback(NUFT_CB_CONF_END, {this, &guard, this->state});
        delete trans_conf;
        trans_conf = nullptr;
        // NOTICE Please Make sure new conf is committed on other nodes, then remove Leader!
        debug_node("Leader finish updating config.\n");
        persister->Dump(guard);
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
        if(Nuke::contains(trans_conf->rem, leader_name)){
            debug_node("Remove Leader %s NOW.\n", leader_name.c_str());
            remove_peer(guard, leader_name);
        }
        invoke_callback(NUFT_CB_CONF_END, {this, &guard, this->state});
        delete trans_conf;
        trans_conf = nullptr;
        debug_node("Follower finish updating config.\n");
        persister->Dump(guard);
    }

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
        raft_message_server = nullptr;
        using namespace std::chrono_literals;
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
        // sync_client_task_queue = std::make_shared<Nuke::ThreadExecutor>(GRPC_SYNC_CONCUR_LEVEL);
        sync_client_task_queue = new Nuke::ThreadExecutor(GRPC_SYNC_CONCUR_LEVEL);
#endif
        start_timepoint = get_current_ms();
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
            delete persister;
        }
            debug_node("Delete Server\n");
            // This may block forever even we called shutdown.
            delete raft_message_server;
            raft_message_server = nullptr;
        {
            GUARD
            debug_node("Wait join\n");
            timer_thread.join();
            debug_node("timer_thread Joined\n");
            if(trans_conf){
                delete trans_conf;
            }
        }
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_SYNC_BARE)
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
