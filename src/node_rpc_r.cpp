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

void RaftNode::do_election(std::lock_guard<std::mutex> & guard) {
    
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
        set_seq_nr(peer, request);
        peer.raft_message_client->AsyncRequestVote(request);
    }
}

int RaftNode::on_vote_request(raft_messages::RequestVoteResponse * response_ptr, const raft_messages::RequestVoteRequest & request) {
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
    response.set_seq(request.seq());

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

    if(!valid_seq(request.seq(), request.initial())){
        debug_node("Out-of-ordered RequestVoteRequest\n");
        return -1;
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

void RaftNode::on_vote_response(const raft_messages::RequestVoteResponse & response) {
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