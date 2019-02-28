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

void RaftNode::apply_conf(const Configuration & conf){
    GUARD
    apply_conf(guard, conf);
}
void RaftNode::apply_conf(std::lock_guard<std::mutex> & guard, const Configuration & conf){
    debug_node("Apply conf: old %u, app %u, rem %u\n", conf.old.size(), conf.app.size(), conf.rem.size());
    for(int i = 0; i < conf.old.size(); i++){
        if(conf.old[i] != name && !(conf.state >= Configuration::State::JOINT_NEW && Nuke::contains(conf.rem, conf.old[i]))){
            add_peer(guard, conf.old[i]);
        } else{
            debug_node("Apply %s fail. conf.state %d\n", conf.old[i].c_str(), conf.state);
        }
    }
    for(int i = 0; i < conf.app.size(); i++){
        if((conf.app[i] != name && (conf.state >= Configuration::State::OLD_JOINT)) ||
            conf.state == Configuration::State::BLANK){
            add_peer(guard, conf.app[i]);
        }
    }
    debug_node("Apply conf end.\n");
}

NuftResult RaftNode::update_configuration(const std::vector<std::string> & app, const std::vector<std::string> & rem){
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

void RaftNode::update_configuration_joint(std::lock_guard<std::mutex> & guard){
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

void RaftNode::on_update_configuration_joint(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry){
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
    invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::JOINT});
    persister->Dump(guard);
}

void RaftNode::on_update_configuration_joint_committed(std::lock_guard<std::mutex> & guard){
    // After joint consensus is committed,
    // Leader can now start to switch to C_{new}.
    debug_node("Joint consensus committed.\n");
    assert(trans_conf);
    trans_conf->state = Configuration::State::JOINT;
    persister->Dump(guard);
    invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::JOINT});
}

void RaftNode::update_configuration_new(std::lock_guard<std::mutex> & guard){
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

void RaftNode::on_update_configuration_new(std::lock_guard<std::mutex> & guard, const raft_messages::LogEntry & entry){
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
    invoke_callback(NUFT_CB_CONF_START, {this, &guard, Configuration::State::JOINT_NEW});
    persister->Dump(guard);
}

void RaftNode::update_configuration_finish(std::lock_guard<std::mutex> & guard){
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

void RaftNode::on_update_configuration_finish(std::lock_guard<std::mutex> & guard){
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
    debug_node("Me(Follower) finish updating config.\n");
    persister->Dump(guard);
}

NodePeer * RaftNode::add_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name) {
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
        peers[peer_name]->seq = SEQ_START;
        peers[peer_name]->raft_message_client = std::make_shared<RaftMessagesClient>(peer_name, this);
#if defined(USE_GRPC_STREAM)
        peers[peer_name]->raft_message_client->handle_response();
#endif
#if defined(USE_GRPC_SYNC) && !defined(USE_GRPC_STREAM) && !defined(USE_GRPC_SYNC_BARE)
        peers[peer_name]->raft_message_client->task_queue = sync_client_task_queue;
#endif
    }
    return peers[peer_name];
}
NodePeer * RaftNode::add_peer(const std::string & peer_name) {
    GUARD
    return add_peer(guard, peer_name);
}
void RaftNode::remove_peer(std::lock_guard<std::mutex> & guard, const std::string & peer_name){
    // Must under the protection of mutex
    if(Nuke::contains(peers, peer_name)){
        debug_node("Remove %s from this->peers.\n", peer_name.c_str());
        peers[peer_name]->send_enabled = false;
        peers[peer_name]->receive_enabled = false;

        // NOTICE There is a log in addpeer.core.log that shows there is a data race between here
        // and `RaftMessagesClientSync::AsyncAppendEntries`'s `!strongThis->raft_node` checking.
        // So we just comment this stmt, because `receive_enabled` can help us do so.
        // peers[peer_name]->raft_message_client->raft_node = nullptr;

        // NOTICE We can't delete here.
        // See `void RaftMessagesStreamClientSync::handle_response():t2` deadlock
        #if defined(USE_GRPC_STREAM)
        peers[peer_name]->raft_message_client->shutdown();
        #endif
        tobe_removed_peers.push_back(peers[peer_name]);
        peers[peer_name] = nullptr;
        peers.erase(peer_name);
    }else{
        debug_node("Already removed %s from this->peers.\n", peer_name.c_str());
    }
}
void RaftNode::remove_peer(const std::string & peer_name){
    GUARD
    return remove_peer(guard, peer_name);
}
void RaftNode::reset_peers(std::lock_guard<std::mutex> & guard){
    while(!this->peers.empty()){
        remove_peer(guard, this->peers.begin()->first);
    }
}
void RaftNode::reset_peers(){
    GUARD
    reset_peers(guard);
}
void RaftNode::clear_removed_peers(std::lock_guard<std::mutex> & guard){
    for(auto np: tobe_removed_peers){
        delete np;
    }
    tobe_removed_peers.clear();
}
void RaftNode::clear_removed_peers(){
    // See `void RaftMessagesStreamClientSync::handle_response():t2` deadlock
    GUARD
    clear_removed_peers(guard);
}

void RaftNode::wait_clients_shutdown(){
    #if defined(USE_GRPC_STREAM)
    while(1){
        for(auto np: tobe_removed_peers){
            if(!np->raft_message_client->is_shutdown()){
                std::this_thread::sleep_for(std::chrono::duration<int, std::milli> {10});
                // debug_node("wait_clients_shutdown with %d support, not end.\n", np->raft_message_client->stop_count.load());
                goto NOT_END;
            }
        }
        break;
        NOT_END:
        continue;
    }
    #else

    #endif
    debug_node("Clients shutdown.\n");
}

bool RaftNode::test_election(){
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

int RaftNode::enough_votes_trans_conf(std::function<bool(const NodePeer &)> f){
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