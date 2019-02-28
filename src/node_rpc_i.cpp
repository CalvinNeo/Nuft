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


void RaftNode::truncate_log(IndexID last_included_index, TermID last_included_term){
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

NuftResult RaftNode::do_install_snapshot(std::lock_guard<std::mutex> & guard, IndexID last_included_index, const std::string & state_machine_state){
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

NuftResult RaftNode::do_install_snapshot(IndexID last_included_index, const std::string & state_machine_state){
    GUARD
    do_install_snapshot(last_included_index, state_machine_state);
}

void RaftNode::do_send_install_snapshot(std::lock_guard<std::mutex> & guard, NodePeer & peer){
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
    set_seq_nr(peer, request);
    peer.raft_message_client->AsyncInstallSnapshot(request);
}

int RaftNode::on_install_snapshot_request(raft_messages::InstallSnapshotResponse* response_ptr, const raft_messages::InstallSnapshotRequest& request) {
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
    response.set_seq(request.seq());

    if(handle_request_routine(guard, request)){
        response.set_term(current_term);
    }else{
        debug_node("Leader ask me to do snapshot, but term disagree.\n");
        goto end;
    }

    if(!valid_seq(request.seq(), request.initial())){
        debug_node("Out-of-ordered InstallSnapshotRequest\n");
        return -1;
    }
    
    // Create snapshot, truncate log, persist
    // NOTICE We must save a copy of `get_base_index()`, because it will be modified.
    debug_node("Leader ask me to do snapshot.\n");
    truncate_log(request.last_included_index(), request.last_included_term());
    logs[0].set_command(NUFT_CMD_SNAPSHOT);
    logs[0].set_data(request.data());
    // The following two asserts MAY NOT STAND!
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

void RaftNode::on_install_snapshot_response(const raft_messages::InstallSnapshotResponse & response) {
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
