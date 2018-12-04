#pragma once

#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <map>

#include "utils.h"
#include "server.h"

#define _HIDE_HEARTBEAT_NOTICE
#define _HIDE_GRPC_NOTICE

typedef std::string NodeName;
typedef int64_t IndexID;
typedef uint64_t TermID;

static constexpr IndexID default_index_cursor = -1; // When no log, index is -1
static constexpr TermID default_term_cursor = 0; // When starting, term is 0
static constexpr uint64_t default_timeout_interval_lowerbound = 1000;
static constexpr uint64_t default_timeout_interval_upperbound = 1500;
static constexpr uint64_t default_heartbeat_interval = 30;
static constexpr uint64_t default_election_fail_timeout_interval = 3000;
#define vote_for_none ""

#define GUARD
#define GUARD std::lock_guard<std::mutex> guard((mut));

struct Log {

};

typedef RaftMessagesClientSync RaftMessagesClient;

struct NodePeer {
    NodeName name;
    bool voted_for_me = false;
    // Index of next log to copy
    IndexID next_index = default_index_cursor;
    // Index of logs already copied
    IndexID match_index = default_index_cursor;
    RaftMessagesClient * raft_message_client = nullptr;
};

struct RaftNode {
    enum NodeState {
        Follower = 0,
        Candidate = 1,
        Leader = 2,
        NotRunning = 3
    };

    // Standard
    TermID current_term = default_term_cursor;
    NodeName vote_for = vote_for_none;
    std::vector<raft_messages::LogEntry> logs;

    // RSM usage
    IndexID commit_index = default_index_cursor;
    IndexID last_applied = default_index_cursor;

    // Leader exclusive

    // Node infomation
    NodeName name;
    NodeState state;
    uint64_t last_tick = 0; // Reset every `default_heartbeat_interval`
    uint64_t elect_timeout_due = 0;
    uint64_t election_fail_timeout_due = 0;

    // Candidate exclusive
    size_t vote_got = 0;

    // Network
    std::map<std::string, NodePeer> peers;
    RaftServerContext * raft_message_server = nullptr;
    std::thread timer_thread;

    // Utils
    // Multiple thread can access the same object,
    // use mutex for protection.
    std::mutex mut;

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
        return enough_votes(vote_got);
    }
    bool enough_votes(size_t vote) const {
        return vote > (peers.size() + 1) / 2;
    }

    void on_timer() {
        // Check heartbeat
        uint64_t current_ms = get_current_ms();

        if (state == NodeState::NotRunning) return;
        if ((state == NodeState::Leader) && current_ms >= default_heartbeat_interval + last_tick) {
            last_tick = current_ms;
            send_heartbeat();
        }
        // Check if leader is timeout
        if ((state != NodeState::Leader) && (current_ms >= elect_timeout_due)) { // Both Leader/Candidate will not check this timeout
            // Start election
            do_election();
        }

        // Check if election is timeout
        if ((state == NodeState::Candidate) && (current_ms >= election_fail_timeout_due)) {
            // In this case, no consensus is reached during the previous election,
            // We restart a election.
            if (enough_votes()) {
                // Maybe there is only me in the cluster...
                become_leader();
            } else {
                debug("Election failed(timeout) with %u votes at term %lu.\n", vote_got, current_term);
                reset_election_timeout();
            }
        }
    }

    void start() {
        printf("Node start.\n");
        if (state != NodeState::NotRunning) {
            return;
        }
        raft_message_server = new RaftServerContext(this);
        state = NodeState::Follower;
        reset_election_timeout();
    }

    void stop() {

    }

    void reset_election_timeout() {
        uint64_t current_ms = get_current_ms();
        uint64_t delta = get_ranged_random(default_timeout_interval_lowerbound, default_timeout_interval_upperbound);
        elect_timeout_due = current_ms + delta;
        #if !defined(_HIDE_HEARTBEAT_NOTICE)
        debug("Sched election after %llums = %llu, current %llu. State %d.\n", delta, elect_timeout_due, current_ms, state);
        #endif
        // `election_fail_timeout_due` should be set only after election begins,
        // Otherwise it will keep triggered in `on_time()`
        election_fail_timeout_due = UINT64_MAX;
    }

    void add_peer(const std::string & peer_name) {
        if (Nuke::exists(peers, peer_name)) {
            debug("Peer already found.\n");
        } else {
            peers[peer_name] = NodePeer{peer_name};
            peers[peer_name].raft_message_client = new RaftMessagesClient(peer_name, this);
        }
    }

    void do_commit_log(const raft_messages::LogEntry & log) {

    }

    void switch_to(NodeState new_state) {
        state = new_state;
    }

    void send_heartbeat() {
        // TODO: optimize for every peer:
        // If already sent Logs, don't do heartbeat this time.
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
        debug("Now I am the Leader of term %llu, because I got %llu votes.\n", current_term, vote_got);
        switch_to(NodeState::Leader);
        vote_for = vote_for_none;

        for (auto & pp : this->peers) {
            // Actually we don't know how many logs have been copied,
            // So we assume no log are copied to peer
            pp.second.match_index = default_index_cursor;
            pp.second.next_index = commit_index + 1;
        }

        // Heartbeat my leadership
        // send_heartbeat();
    }

    void do_election() {
        GUARD
        
        switch_to(NodeState::Candidate);
        current_term++;
        vote_got = 1; // Vote for myself
        vote_for = name;

        uint64_t current_ms = get_current_ms();
        election_fail_timeout_due = current_ms + default_election_fail_timeout_interval;
        debug("Start election term: %lu, will timeout in %llu ms.\n", current_term, default_election_fail_timeout_interval);
        // Same reason. Ref. `reset_election_timeout`
        elect_timeout_due = UINT64_MAX;

        for (auto & pp : peers) {
            raft_messages::RequestVoteRequest request;
            request.set_name(name);
            request.set_term(current_term);
            request.set_last_log_index(last_log_index());
            request.set_last_log_term(last_log_term());
            
            NodePeer & peer = pp.second;
            pp.second.voted_for_me = false;
            debug("Send RequestVoteRequest to %s\n", peer.name.c_str());
            assert(peer.raft_message_client != nullptr);
            peer.raft_message_client->AsyncRequestVote(request);
        }
    }

    raft_messages::RequestVoteResponse on_vote_request(const raft_messages::RequestVoteRequest & request) {
        // When receive a `RequestVoteRequest` from peer

        GUARD
        raft_messages::RequestVoteResponse response;
        response.set_name(name);
        response.set_vote_granted(false);

        if (request.term() > current_term) {
            // New leader
            debug("Quit election because found newer term from %s of %llu, me %llu.\n", request.name().c_str(), request.term(), current_term);
            become_follower(request.term());
            reset_election_timeout();
        } else if (request.term() < current_term) {
            // Reject. A request of old term
            goto end;
        }

        if (vote_for != vote_for_none && vote_for != request.name()) {
            // Reject. Already vote other
            goto end;
        }

        if (request.last_log_term() < last_log_term()) {
            goto end;
        }
        if (request.last_log_term() == last_log_term() && request.last_log_index() < last_log_index()) {
            goto end;
        }

        // Vote.
        vote_for = request.name();
        response.set_vote_granted(true);
end:
        // Must set term here, cause `become_follower` may change it
        response.set_term(current_term);
        debug("Respond to %s with %s\n", request.name().c_str(), response.vote_granted() ? "YES" : "NO");
        return response;
    }

    void on_vote_response(const raft_messages::RequestVoteResponse & response) {
        // Called from server.h
        // When a candidate receives vote from others

        GUARD
        if (state != NodeState::Candidate) {
            return;
        }

        if(!Nuke::exists(peers, response.name())){
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
        } else if (response.term() > current_term) {
            become_follower(response.term());
        }
    }

    void do_append_entries(bool heartbeat) {
        // Send `AppendEntriesRequest` to all peer.
        // When heartbeat is set, this `AppendEntriesRequest` function as noly heartbeat

        GUARD
        #if !defined(_HIDE_HEARTBEAT_NOTICE)
        debug("Begin sending %s AppendEntriesRequest.\n", heartbeat ? "heartbeat": "");
        #else
        if(!heartbeat){
        debug("Begin sending %s AppendEntriesRequest.\n", heartbeat ? "heartbeat": "");
        }
        #endif
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
            if (!heartbeat) {
                for (IndexID i = peer.next_index; i < logs.size(); i++) {
                    raft_messages::LogEntry & entry = *(request.add_entries());
                    entry = logs[i];
                }
            }
            peer.raft_message_client->AsyncAppendEntries(request);
        }
    }

    raft_messages::AppendEntriesResponse on_append_entries_request(const raft_messages::AppendEntriesRequest & request) {
        // When a Follower/Candidate receive `AppendEntriesResponse` from Leader,
        // Try append entries, then return a `AppendEntriesResponse`

        GUARD
        raft_messages::AppendEntriesResponse response;
        response.set_name(name);
        response.set_success(false);

        IndexID prev_i = request.prev_log_index(), prev_j = 0;

        reset_election_timeout();

        if (request.term() < current_term) {
            goto end;
        }

        if (request.term() > current_term) {
            // Discover a request with newer term
            debug("Become Follower because AppendEntriesRequest from %s with newer term %llu\n", request.name().c_str(), request.term());
            become_follower(request.term());
        }
        if (request.term() == current_term && state == NodeState::Candidate) {
            // In election and a Leader is determined
            debug("Become Follower because AppendEntriesRequest from %s at my candidate term %llu\n", request.name().c_str(), request.term());
            become_follower(request.term());
        }

        // request.prev_log_index() == -1 happens at the very beginning when a Leader with no Log, and send heartbeats to others.

        if (request.prev_log_index() >= 0 && request.prev_log_index() >= logs.size()) {
            // I don't actually have the `prev_log_index()`,
            // Failed.
            debug("AppendEntries fail because I don't have prev_log_index = %llu, my log size = %llu.\n", request.prev_log_index(), logs.size());
            goto end;
        }

        if (request.prev_log_index() >= 0 && logs[request.prev_log_index()].term() != request.prev_log_term()) {
            // "If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it"
            assert(commit_index < request.prev_log_index());
            debug("AppendEntries fail because my term[prev_log_index=%lld] = %llu, Leader's = %llu.\n", request.prev_log_index(),
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
        if (prev_j < request.entries_size()) {
            // Copy the rest entries
            logs.insert(logs.end(), request.entries().begin() + prev_j, request.entries().end());
        }

        if (request.leader_commit() > commit_index) {
            // Update commit
            commit_index = std::min(request.leader_commit(), IndexID(logs.size() - 1));
        }

succeed:
        response.set_success(true);
end:
        // Must set term here, cause `become_follower` may change it
        response.set_term(current_term);
        response.set_last_log_index(last_log_index());
        response.set_last_log_term(last_log_term());
        return response;
    }

    void on_append_entries_response(const raft_messages::AppendEntriesResponse & response) {
        // When Leader receive `AppendEntriesResponse` from others
        GUARD
        if (state != NodeState::Leader) {
            return;
        }

        if (response.term() > current_term) {
            // New leader
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
            return;
        }
        // Update commit
        if (commit_index >= response.last_log_index()) {
            // `commit_index` still ahead of peer's index.
            // This peer can't contribute to `commit_vote`
            return;
        }
        // See `on_append_entries_request`
        assert(logs[response.last_log_index()].term() == response.last_log_term());
        if (current_term != logs[response.last_log_index()].term()) {
            // According to <<In Search of an Understandable Consensus Algorithm>>
            // "Raft never commits log entries from previous terms by counting replicas.
            // Only log entries from the leader’s current term are committed by counting replicas"
            return;
        }

        // Now we can try to commit to `response.last_log_index()`
        size_t commit_vote = 0;
        for (auto & pp : peers) {
            if (pp.second.match_index >= response.last_log_index()) {
                commit_vote++;
            }
        }
        if (enough_votes(commit_vote)) {
            commit_index = response.last_log_index();
        }
    }

    RaftNode(const std::string & addr) : state(NodeState::NotRunning), name(addr) {
        using namespace std::chrono_literals;
        timer_thread = std::thread([&]() {
            while (1) {
                this->on_timer();
                std::this_thread::sleep_for(10ms);
            }
        });
        timer_thread.detach();
    }
    RaftNode(const RaftNode &) = delete;
    ~RaftNode() {
        for (auto & pp : this->peers) {
            NodePeer & peer = pp.second;
            delete (peer.raft_message_client);
        }
    }

    void _list_peers(){
        for (auto & pp : this->peers) {
            printf("==> %s\n", pp.second.name.c_str());
        }
    }
};


RaftNode * make_raft_node(const std::string & addr);