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

#include "node.h"
#include "grpc_utils.h"

RaftNode * make_raft_node(const std::string & addr) {
    RaftNode * node = new RaftNode(addr);
    return node;
}

RaftNode::RaftNode(const std::string & addr) : state(NodeState::NotRunning), name(addr) {
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

RaftNode::~RaftNode() {
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
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_STREAM)
#if !defined(USE_GRPC_SYNC_BARE)
    // TODO this will sometimes blocks at thread pool.
    // I think it may because of:
    // 1. ~RaftNode locks mut
    // 2. delete `sync_client_task_queue` will wait until all pending RPCs are handled
    // 3. one response arrived, e.g. call `on_append_entries_response`
    // 4. it will try to acquire mut again
    // 5. deadlock
    delete sync_client_task_queue;
#endif
#endif
    debug_node("Destruct finish\n");
}

void RaftNode::do_apply(bool from_snapshot) {
    GUARD
    do_apply(guard, from_snapshot);
}
void RaftNode::do_apply(std::lock_guard<std::mutex> & guard, bool from_snapshot) {
    assert(!(from_snapshot && gl(last_applied + 1).command() != NUFT_CMD_SNAPSHOT));
    debug_node("Do apply from %lld to %lld\n", last_applied + 1, commit_index);
    for(IndexID i = last_applied + 1; i <= commit_index; i++){
        if(gl(i).command() == NUFT_CMD_NOP){
            debug_node("Log[%lld] is NOP, don't apply.\n", i);
            last_applied = i;
            continue;
        }
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

void RaftNode::reset_election_timeout() {
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

void RaftNode::on_timer() {
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

void RaftNode::run(std::lock_guard<std::mutex> & guard) {
    debug_node("Run node. Switch state to Follower.\n");
    if (state != NodeState::NotRunning) {
        return;
    }
    start_timepoint = get_current_ms();
    paused = false;
    state = NodeState::Follower;
    last_seq = 0;
    persister->Dump(guard, true);
    reset_election_timeout();
}

void RaftNode::safe_leave(std::lock_guard<std::mutex> & guard){
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

bool RaftNode::valid_seq(uint64_t seq, bool initial){
    #if !defined(USE_MORE_REMOVE)
    return true;
    #endif
    if(initial){
        last_seq = seq;
        return true;
    }else if(seq > last_seq){
        last_seq = seq;
        return true;
    }
    return false;
}