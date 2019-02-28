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

#include "test_utils.inc.h"

TEST(Network, RedirectToLeader){
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * ob = PickNode({RN::Follower, RN::Candidate});
    printf("GTEST: PickNode %s\n", ob->name.c_str());
    std::string leader_name = ob->get_leader_name();
    ASSERT_TRUE(leader_name != "");
    for(auto nd: nodes){
        if(nd->name == leader_name){
            goto OK;
        }
    }
    ASSERT_TRUE(false);
OK:
    FreeRaftNodes();
    ((void)0);
}

TEST(Election, Basic){
    int n = 3;
    ASSERT_GT(n, 1);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    // It is important to wait for a while and let the election ends in all node.
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(4s);
    int leader_cnt = CountLeader();
    ASSERT_EQ(leader_cnt, 1);
    FreeRaftNodes();
}

TEST(Election, Normal){
    int n = 10;
    ASSERT_GT(n, 1);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    // It is important to wait for a while and let the election ends in all node.
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(4s);
    int leader_cnt = CountLeader();
    ASSERT_EQ(leader_cnt, 1);
    FreeRaftNodes();
}

TEST(Election, LeaderLost){
    int n = 4;
    ASSERT_GT(n, 1);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);

    // Now shutdown Leader
    RaftNode * victim = PickNode({RN::Leader});
    ASSERT_TRUE(victim != nullptr);
    DisableNode(victim); // We close leader and observe another node.
    // Can't wait here, or we will miss the start of election. 
    ASSERT_EQ(CountLeader(), 0);
    
    // Election
    RaftNode * ob = PickNode({RN::Follower});
    ASSERT_NE(ob, nullptr);
    WaitElectionStart(ob, {RaftNode::ELE_VOTE, RaftNode::ELE_NEW, RaftNode::ELE_AGA});
    printf("GTEST: Observe election through %s\n", ob->name.c_str());
    WaitElection(ob);
    std::this_thread::sleep_for(1s);
    RaftNode * new_leader = PickNode({RN::Leader});
    ASSERT_EQ(CountLeader(), 1);

    // Now old Leader came back
    EnableNode(victim);
    std::this_thread::sleep_for(2s);
    ASSERT_EQ(PickNode({RN::Leader}), new_leader);

    // Now two nodes disconnect, no leader can be elected.
    DisableNode(victim);
    DisableNode(new_leader);
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CountLeader(), 0);
    
    FreeRaftNodes();
}

TEST(Commit, Normal){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    TermID original_term = leader->current_term;

    int ln = 5;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        if(support != MajorityCount(n)){
            // See Persist.CrashAll
            printf("GTEST: support %d is not enough. logs[%d] is not committed. term %llu, original_term %llu\n", 
                support, i, leader->current_term, original_term);
        }else{
            printf("GTEST: support %d\n", support);
        };
        ASSERT_GE(support, MajorityCount(n));
    }
    FreeRaftNodes();
}

TEST(Commit, FollowerLost){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    TermID original_term = leader->current_term;
    
    // Disable 1 node
    RaftNode * victim = PickNode({RN::Follower, RN::Candidate});
    ASSERT_NE(victim, nullptr);
    printf("GTEST: Disable 1\n");
    DisableNode(victim);
    print_state();

    int ln = 4;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        if(support != MajorityCount(n)){
            // See Persist.CrashAll
            printf("GTEST: support %d is not enough. logs[%d] is not committed. term %llu, original_term %llu\n", 
                support, i, leader->current_term, original_term);
        }else{
            printf("GTEST: support %d\n", support);
        };
        // Shutting one node will not affect.
        ASSERT_GE(support, MajorityCount(n));
    }

    // Disable 2 nodes
    RaftNode * victim2 = PickNode({RN::Follower, RN::Candidate});
    printf("GTEST: Now disable and mute victim2 %s.\n", victim2->name.c_str());
    DisableReceive(victim2, {0, 1, 2});
    DisableSend(victim2, {0, 1, 2});
    // victim2 will keep electing but never succeed. 
    print_state();
    leader->do_log("DOOMED"); // Index = 4
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    // A majority of nodes disconnected, this entry should not be committed.
    ASSERT_EQ(CheckCommit(ln, "DOOMED"), 0);
    ASSERT_EQ(leader->commit_index, ln - 1);
    
    // Enable victim2
    printf("GTEST: Now enable victim2.\n");
    EnableReceive(victim2, {0, 1, 2});
    EnableSend(victim2, {0, 1, 2});
    print_state();
    // Leader lost leadership because of victim2's reconnection, however, leader will soon re-gain its leadership.
    WaitElection(victim2);
    if(leader->state != RN::Leader){
        // leader may already win the election before victim2 timeout.
        // see followerlost.log
        WaitElection(leader);
    }
    // Give time to AppendEntries
    std::this_thread::sleep_for(1s);
    // We use log entry "A" to force a commit_index advancing, otherwise "DOOMED" wont'be committed. 
    // ref `on_append_entries_response`
    leader->do_log("A");
    std::this_thread::sleep_for(1s);
    printf("GTEST: now check entries.\n");
    // Now, this entry should be committed, because victim2 re-connected.
    ASSERT_GE(CheckCommit(ln + 1, "A"), MajorityCount(n));
    ASSERT_EQ(leader->commit_index, ln + 1);
    FreeRaftNodes();
}

TEST(Commit, LeaderLost){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_NE(leader, nullptr);
    TermID original_term = leader->current_term;
    
    int ln1 = 4, ln2 = 4;
    for(int i = 0; i < ln1; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        int retry = 0;
        while(1){
            std::this_thread::sleep_for(1s);
            int support = CheckCommit(i, logstr);
            int maj = MajorityCount(n);
            if(support < maj){
                // See Persist.CrashAll
                printf("GTEST: retry %d, support %d < %d is not enough. logs[%d] is not committed. term %llu, original_term %llu\n", 
                    retry, support, maj, i, leader->current_term, original_term);
            }else{
                printf("GTEST: support %d\n", support);
                break;
            }
            retry++;
            ASSERT_LT(retry, 4);
        }
        int support = CheckCommit(i, logstr);
        ASSERT_GE(support, MajorityCount(n));
    }
    ASSERT_EQ(leader->commit_index, ln1 - 1);
    
    // Disable leader
    DisableNode(leader);
    RaftNode * ob = PickNode({RN::Follower, RN::Candidate});
    WaitElection(ob);

    std::this_thread::sleep_for(1s);
    RaftNode * new_leader = PickNode({RN::Leader});
    ASSERT_NE(new_leader, nullptr);
    for(int i = ln1; i < ln1 + ln2; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        new_leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        if(support < MajorityCount(n)){
            // See Persist.CrashAll
            printf("GTEST: support %d is not enough. logs[%d] is not committed. term %llu, original_term %llu\n", 
                support, i, leader->current_term, original_term);
        }else{
            printf("GTEST: support %d\n", support);
        };
        ASSERT_GE(support, MajorityCount(n));
    }
    ASSERT_EQ(new_leader->commit_index, ln1 + ln2 - 1);

    FreeRaftNodes();
}

TEST(Commit, NetworkPartition){
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    int l1 = PickIndex({RN::Leader});
    ASSERT_NE(l1, -1);
    
    nodes[l1]->do_log("1");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(nodes[l1]->commit_index, 0);

    // Network partition
    printf("GTEST: START Network Partition\n");
    std::vector<int> p1 = {l1, (l1+1)%n};
    std::vector<int> p2 = {(l1+2)%n, (l1+3)%n, (l1+4)%n};
    NetworkPartition(p1, p2);
    printf("GTEST: Do logs[2]\n");
    nodes[l1]->do_log("2");
    // seq.networkpartition.checklog.log shows it is not enough
    // std::this_thread::sleep_for(TimeEnsureNoElection());
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CheckCommit(0, "1"), 5);
    ASSERT_EQ(CheckCommit(1, "2"), 0);
    ASSERT_EQ(CheckLog(1, "2"), 2);
    ASSERT_EQ(nodes[l1]->commit_index, 0);

    // NOTICE In some tests, it seems that we the following a early election may not be captured by `WaitElection`
    // WaitElection(nodes[(l1+2)%n]);
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    print_state();
    ASSERT_EQ(CountLeader(), 2);
    // There are 2 Leaders in both parts.
    int l2 = PickIndex({RN::Leader}, 1);
    if(l2 == l1){
        l2 = PickIndex({RN::Leader}, 0);
    }
    ASSERT_NE(l1%n, l2%n);
    nodes[l2]->do_log("3");
    int retry = 0;
    while(nodes[l2]->commit_index != 1){
        std::this_thread::sleep_for(1s);
        ASSERT_LE(retry, 4);
    }
    ASSERT_EQ(CheckCommit(1, "3"), -1);
    
    RecoverNetworkPartition(p1, p2);
    std::this_thread::sleep_for(2s);
    printf("GTEST: Now l1 %d should be Follower, l2 %d should be Leader.\n", l1, l2);
    // Now l2 is the leader
    print_state();
    ASSERT_EQ(CheckCommit(1, "3"), 5);
    ASSERT_EQ(nodes[l1]->commit_index, 1);
    FreeRaftNodes();
}

TEST(Commit, LeaderChange){
    // Use part of the Figure8 in the Raft Paper
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    // NOTICE A test in delayed_start_election.log shows a node(name 7104) can start very late and miss the election.
    // And GRPC error 14 may happen. This may due to a strange GRPC problem.
    // These errors are also shown in combined_err.log and combined_err2.log
    // This problems may lead to the problem in leaderchange7104delay.log and delayed_start_election.log etc.
    int l1 = PickIndex({RN::Leader});
    ASSERT_NE(l1, -1);
    
    // (a)
    // S1 = l1, S2 = l1 + 1, ...
    nodes[l1]->do_log("1");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(nodes[l1]->commit_index, 0);
    print_state();

    // S1 replicate successfully only to S2, then crashed.
    printf("GTEST: START Network Partition\n");
    DisconnectAfterReplicateTo(nodes[l1], "2", {l1+1});
    printf("GTEST: Crash\n");
    // NOTICE There are some Failures(logged in leaderchangestrangeblock.log and leaderchange7104delay.log) here, shows l1 replicate entry "2" to l1+1,
    // But l1+1 doesn't respond for about 200ms(log shows no `on_append_entries_request` is handled), and a GRPC error comes very later.
    // This problem is also mentioned in Persist.CrashAll. They may due to some gRPC calls are delayed.
    print_state();
    // Leader is crashed now, we don't count its vote in `CheckLog` abd `CheckCommit`.
    ASSERT_EQ(CheckCommit(0, "1"), 4);
    ASSERT_EQ(CheckCommit(1, "2"), 0);
    // Only S1 and S2 has entry "2", S1 is Crashed, so only S2 have entry "2"
    ASSERT_EQ(CheckLog(1, "2"), 1);
    ASSERT_EQ(nodes[l1]->commit_index, 0);
    
    // (b)
    // S2-S5 timeout.
    WaitElection(nodes[(l1+2)%n]);
    std::this_thread::sleep_for(1s);
    // S1 crashed. so return 1 rather than 2.
    ASSERT_EQ(CountLeader(), 1);
    int l2 = PickIndex({RN::Leader}, 0);
    ASSERT_NE(l1%n, l2%n);
    printf("GTEST: Old Leader %d, New Leader %d\n", l1, l2);
    print_state();
    // S2's wrong entry (term 2, index 2) was not removed before we DisableSend from l2 to l1+1.
    DisconnectAfterReplicateTo(nodes[l2], "3", {});
    print_state();
    if(l2 % n == (l1 + 1) % n){
        ASSERT_EQ(CheckCommit(2, "3"), 0);
    }else{
        // nodes[l1+1]'s log entry "2" is overwritten by nodes[l2]
        ASSERT_EQ(CheckCommit(1, "3"), 0);
    }
    FreeRaftNodes();
}

TEST(Config, AddPeer){
    int n = 4;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    RaftNode * nnd = AddRaftNode(new_port_base);
    leader->update_configuration({nnd->name}, {});
    WaitConfig(leader, NUFT_CB_CONF_END, {RN::Leader});
    ASSERT_EQ(leader->peers.size(), n);
    ASSERT_EQ(nnd->peers.size(), n);
    FreeRaftNodes();
}


TEST(Config, Initialize){
    using namespace std::chrono_literals;
    FreeRaftNodes();
    int n = 3;
    nodes.resize(n);

    for(int i = 0; i < n; i++){
        nodes[i] = new RaftNode(std::string("127.0.0.1:") + std::to_string(port_base + i));
    }
    for(int i = 0; i < n; i++){
        std::vector<std::string> app;
        for(int j = 0; j < n; j++){
            if(j == i){
                continue;
            }
            app.push_back(nodes[j]->name);
        }
        auto conf = RaftNode::Configuration{app, {}, {nodes[i]->name}};
        conf.state = RaftNode::Configuration::State::BLANK;
        nodes[i]->apply_conf(conf);
    }
    for(auto nd: nodes){
        nd->run();
    }
    std::this_thread::sleep_for(3s);
    ASSERT_EQ(CountLeader(), 1);
    ASSERT_EQ(nodes[0]->peers.size(), 2);
    FreeRaftNodes();
}

TEST(Config, DelPeer){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    RaftNode * victim = PickNode({RN::Follower});
    ASSERT_NE(victim, leader);
    leader->update_configuration({}, {victim->name});
    WaitConfig(leader, NUFT_CB_CONF_END, {RN::Leader});
    victim->safe_leave();
    DisableNode(victim);
    RaftNode * ob = PickNode({RN::Follower});
    ASSERT_EQ(ob->peers.size(), 1);
    ASSERT_EQ(leader->peers.size(), 1);
    FreeRaftNodes();
}

TEST(Config, DelLeader){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    RaftNode * ob = PickNode({RN::Follower});
    ASSERT_NE(ob, leader);
    print_state();
    printf("GTEST: Start conf.\n");
    leader->update_configuration({}, {leader->name});
    printf("GTEST: Now wait leader finish conf trans.\n");
    // NOTICE According to the Raft Paper, If we remove Leader from cluster,
    // We can only remove it when the new conf is committed.
    // Otherwise, A common consequence is a newly elected Leader can't commit the new conf,
    // Because it is created by a previous Leader. And we wait here.
    WaitConfig(ob, NUFT_CB_CONF_END, {RN::Follower});
    printf("GTEST: Now switched to new conf.\n");
    leader->safe_leave();
    print_state();
    ASSERT_EQ(ob->peers.size(), 1);
    WaitElection(ob);
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CountLeader(), 1);
    print_state();
    FreeRaftNodes();
}

TEST(Config, AddPeerLeaderLost){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    RaftNode * nnd = AddRaftNode(new_port_base);
    leader->update_configuration({nnd->name}, {});
    printf("GTEST: Now begin to replicate joint consensus.\n");
    print_state();
    // Give time for leader to re-win a election, and replicate. 
    // see addpeerleaderlost2.log
    WaitAny(leader, NUFT_CB_CONF_START, [&](NUFT_CB_TYPE type, NuftCallbackArg * args){
            if(args->a1 == RaftNode::Configuration::State::JOINT){
                // If we crash at JOINT, we must ensure the new leader will resume the rest stages of conf trans.
                // Now joint consensus is committed, but we Disable Leader.
                printf("GTEST: Joint consensus committed, disable Leader Again.\n");
                // NOTICE the lambda holds the lock of raft node.
                print_state();
                DisableNode(leader, *(args->lk));
                return 1;
            }
            return 0;
        });
    print_state();
    WaitElection(nnd);
    std::this_thread::sleep_for(1s);
    print_state();
    RaftNode * leader2 = PickNode({RN::Leader});
    leader2->do_log("Help commit former entries.\n");
    WaitConfig(nnd, NUFT_CB_CONF_END, {RN::Leader, RN::Follower});
    print_state();
    ASSERT_EQ(nnd->peers.size(), n);
    // Though leader disconnected and not committed join consensus, it already has joint consensus entry and will obey it.
    ASSERT_EQ(leader->peers.size(), n);
    FreeRaftNodes();
}

TEST(Persist, CrashAll){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    print_state();
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    TermID original_term = leader->current_term;

    int ln = 4;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log") + std::to_string(i);
        printf("GTEST: Do logs[%d] = '%s'\n", i, logstr.c_str());
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        // NOTICE As is mentioned in Commit.NetworkPartition, entries are ONLY guarenteed to persist when they are committed.
        // I intentionally CheckCommit here and somewhere else, because I think entries should be committed smoothly.
        // However, there is a slight chance that RPC calls are delayed, and this can lead to a election timeout.
        // When a new Leader without entry A is elected. It may erase entry A on the cluster. 
        // For example delayed_heartbeat.log.
        if(support != MajorityCount(n)){
            printf("GTEST: support %d is not enough. logs[%d] is not committed. term %llu, original_term %llu\n", 
                support, i, leader->current_term, original_term);
        }else{
            printf("GTEST: support %d\n", support);
        }
        ASSERT_GE(support, MajorityCount(n));
    }
    for(int iter = 0; iter < 1; iter++){
        print_state();
        printf("GTEST: Now crash all\n");
        for(auto nd: nodes){
            CrashNode(nd);
        }
        print_state();
        printf("GTEST: Now restart all\n");
        for(auto nd: nodes){
            RecoverNode(nd);
        }
        print_state();
    }
    printf("GTEST: Now Whip\n");
    leader->do_log("Whip");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CheckCommit(ln, "Whip"), n);
    FreeRaftNodes();
}

TEST(Persist, CrashLeader){
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);


    FreeRaftNodes();
}

TEST(Persist, AddPeer){
    int n = 4;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    RaftNode * nnd = AddRaftNode(new_port_base);
    leader->update_configuration({nnd->name}, {});
    printf("GTEST: Wait Config at %s.\n", nnd->name.c_str());
    // NOTICE Can't WaitAny with Leader.
    // Leader may change to 7103 under the following situation, see stream.Persist.Addpeer.LeaderChanged.log
    //               Name        State  Term  log size  commit lastapp   peers    run  trans
    // 127.0.0.1:7100       Leader     2         2       0      -1       4      T      4
    // 127.0.0.1:7101     Follower     2         1       0       0       4      T      2
    // 127.0.0.1:7102     Follower     2         1      -1      -1       4      T      2
    // 127.0.0.1:7103     Follower     2         1      -1      -1       4      T      2
    // 127.0.0.1:7200     Follower     2         1      -1      -1       4      T      2
    WaitAny(nnd, NUFT_CB_CONF_START, [&](NUFT_CB_TYPE type, NuftCallbackArg * args){
            if(args->a1 == RaftNode::Configuration::State::JOINT_NEW){
                // Now joint consensus is committed
                return 1;
            }
            return 0;
        });
    print_state();
    printf("GTEST: Joint consensus committed, crash Leader.\n");
    CrashNode(leader);
    // Election Timeout
    std::this_thread::sleep_for(2s);
    printf("GTEST: Recover Leader.\n");
    RecoverNode(leader);
    print_state();
    // We wait for several seconds and everything should be OK.
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(leader->peers.size(), n);
    ASSERT_EQ(nnd->peers.size(), n);
    FreeRaftNodes();
}

TEST(Persist, FrequentCrash){
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * leader = PickNode({RN::Leader});
    ASSERT_TRUE(leader != nullptr);

    int tot = 100;
    int crash_interval = 30;
    for(int i = 0; i < tot; i++){
        printf("GTEST: Do logs[%d]\n", i);
        int retry = 0;
        // NOTICE In a early version, the client blocks here,
        // Because the client called `leader->do_log` when leader is no longer the Leader.
        // So these entries has gone missing. Ref persist.frequent.log
        while((!leader) || leader->do_log(std::to_string(i) + ";") < 0){
            std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
            leader = PickNode({RN::Leader});
            ASSERT_LT(retry, 3);
        }
        // We appen and apply one-by-one, in case somw entries are overwirtten.
        WaitApplied(leader, i);
        if(i % crash_interval == 1){
            for(auto nd: nodes) {
                CrashNode(nd);
            }
            print_state();
            for(auto nd: nodes) {
                RecoverNode(nd);
            }
            print_state();
        }
    }
    printf("GTEST: Whip\n");
    leader->do_log("Whip\n");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(leader->commit_index, tot);
    ASSERT_EQ(leader->last_applied, tot);
    FreeRaftNodes();
}

void GenericTest(int tot = 100, int n = 5, int snap_interval = -1, bool test_lost = false, bool test_crash = false, int clients = 1){
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CountLeader(), 1);

    std::unordered_set<RaftNode *> lost_nodes;
    std::unordered_set<RaftNode *> crash_nodes;

    logs.clear();
    storage.clear();
    correct_storage.clear();
    std::atomic<int> current_tid;
    current_tid.store(0);

    for(auto nd: nodes){
        MonitorApplied(nd, [snap_interval](NuftCallbackArg * arg, ApplyMessage * applymsg, std::lock_guard<std::mutex> & guard){
            RaftNode * nd = arg->node;
            IndexID index = applymsg->index;
            if(applymsg->from_snapshot){
                printf("GTEST: Apply snapshot logs[%d] = '%s'\n", index, applymsg->data.c_str());
                storage[0] = applymsg->data;
            }else{
                printf("GTEST: Apply logs[%d] = '%s'\n", index, applymsg->data.c_str());
                std::vector<std::string> l = Nuke::split(applymsg->data, "=");

                int tid = atoi(l[0].c_str());

                if(logs.find(index) != logs.end()){
                    // If this entry is already applied, they must identical.
                    printf("GTEST: Repeated apply at %lld.\n", index);
                    ASSERT_EQ(logs[index].data, applymsg->data);
                }else{
                    RegisterLog(guard, applymsg->data, nd, index, tid);
                    if(!Nuke::contains(correct_storage, tid)){
                        correct_storage[tid] = "";
                    }
                    correct_storage[tid] += applymsg->data;
                    if(!Nuke::contains(storage, tid)){
                        storage[tid] = "";
                    }
                    storage[tid] += applymsg->data;
                    printf("GTEST: storage = '%s'\n", storage[tid].c_str());
                }
                // TODO Check if we apply in order(one-by-one)

                logs[index].applied = true;

                if(snap_interval > 0 && index % snap_interval == snap_interval - 1){
                    // Move to MonitorApply, Under protection from `monitor_mut`.
                    // NOTICE deadlock may happend here, because `do_apply` may hold lock.
                    printf("GTEST: APPLIED of %d is %d. Now do snapshot\n", index, logs[index].applied);
                    print_state();
                    arg->node->do_install_snapshot(*(arg->lk), index, storage[tid]);
                }
            }
        });
    }

    if(test_lost){
        RaftNode * victim = PickNode({RN::Follower, RN::Leader, RN::Candidate});
        lost_nodes.insert(victim);
        DisableNode(victim);
    }
    if(test_crash){
        RaftNode * victim = PickNode({RN::Follower, RN::Leader, RN::Candidate});
        crash_nodes.insert(victim);
        CrashNode(victim);
    }
    print_state();
    // NOTICE After that, Leader may crash or lost
    auto inner_loop = [&](){
        int tid = current_tid.fetch_add(1);
        for(int i = 0; i < tot; i++){
            std::string s = std::to_string(tid) + "=" + std::to_string(i) + ";";
            // NOTICE There may be so many tasks in `task_queue`,
            // that we can't send heartbeat immediately.
            // Then one of our nodes timeout and start an election.
            int retry = 0;
            while(!CountLeader() == 1){
                // In some cases, `TimeEnsureElection` provides not enough time.
                // ref lostandcrash_notenoughtime.log
                std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
                retry++;
                ASSERT_LT(retry, 4);
            }
            RaftNode * leader = PickNode({RN::Leader});
            // NOTICE We must use this callback to RegisterLog.
            // Otherwise there's a slight chance that RaftNode will apply before we RegisterLog. 
            int log_id;
            while(1){
                // NOTICE We can't RegisterLog here! We must Register them when applied.
                // In a earlier version We `RegisterLog` the Whip entry is the `tot * clients`th entry,
                // However, previous logs may not successfully applied due to Leader change.
                // And we just assume they will be succefully added, that we `RegisterLog` at `do_log`,
                // rathen than `MonitorApplied`. 
                if(leader){
                    log_id = leader->do_log(s);
                }
                if(log_id < 0){
                    printf("GTEST: Do logs[%d] Error\n", log_id);
                    std::this_thread::sleep_for(TimeEnsureElection());
                    leader = PickNode({RN::Leader});
                }else{
                    break;
                }
            }
            printf("GTEST: Do logs[%d] from thread %d = '%s'\n", log_id, tid, s.c_str());
        }
    };

    std::thread * ths = new std::thread[clients](inner_loop);
    for(int i = 0; i < clients; i++){
        ths[i].join();
    }
    for(auto nd: lost_nodes){
        printf("GTEST: EnableNode %s\n", nd->name.c_str());
        EnableNode(nd);
    }
    for(auto nd: crash_nodes){
        RecoverNode(nd);
    }
    printf("GTEST: Thread Finished.\n");
    print_state();
    // Give enough time.
    // Now we assert that all committed entries are applied, though some entries we append by `do_log` can gone missing.
    std::this_thread::sleep_for(std::chrono::duration<int, std::milli>{10 * tot * clients});
    RaftNode * leader = PickNode({RN::Leader});
    // Prevent "can't commit logs replicated by previous Leaders".
    NuftResult whip_id = leader->do_log("0=Whip");
    ASSERT_LE(whip_id, tot * clients);
    printf("GTEST: Whip at %d\n", whip_id);
    std::this_thread::sleep_for(std::chrono::duration<int, std::milli>{30 * tot * clients});
    delete [] ths;
    print_state();
    for(int i = 0; i < clients; i++){
        printf("GTEST: storage[%d] is '%s'\n", i, storage[i].c_str());
    }
    leader = PickNode({RN::Leader});
    ASSERT_NE(leader, nullptr);
    ASSERT_EQ(leader->commit_index, leader->last_applied);
    ASSERT_LE(leader->commit_index, tot * clients);
    for(int i = 0; i < clients; i++){
        ASSERT_EQ(storage[i], correct_storage[i]);
    }
    if(snap_interval > 0){
        // ASSERT_LE(leader->logs.size(), snap_interval);
    }
    for(auto nd: nodes){
        StopMonitorApplied(nd);
    }
    FreeRaftNodes();
}

TEST(Snapshot, Basic){
    GenericTest(100, 5, 30, false, false, 1);
}

TEST(Snapshot, Lost){
    // NOTICE Important issue about reordered rpc.
    // snapshot.lost.fail.log elaborates how a reordered RPC call may lead to inconsistent,
    // By erasing a committed entry.
    // NOTICE Someone says Raft requires ordered stream. See https://stackoverflow.com/questions/54310611/how-to-handle-reordered-rpc-in-raft
    // NOTICE However, others think this is due to a misimplementation.
    // See https://thesquareplanet.com/blog/raft-qa/
    // NOTICE seq.concurrent.log shows even we refuse reordered RPC, errors can still happen.
    GenericTest(200, 5, 30, true, false, 1);
}

TEST(Snapshot, LostAndCrash){
    GenericTest(200, 5, 30, true, true, 1);
}

TEST(Concurrent, Basic){
    GenericTest(200, 5, -1, false, false, 3);
}

TEST(Concurrent, LostAndCrash){
    GenericTest(200, 5, -1, true, true, 3);
}

int main(int argc, char ** argv){
    std::future<int> future = std::async(std::launch::async, [&](){
        testing::InitGoogleTest(&argc, argv);
        auto r = RUN_ALL_TESTS();
        FreeRaftNodes();
        return r;
    });
    std::future_status status = future.wait_for(std::chrono::seconds(300));
    assert(status == std::future_status::ready);
    return future.get();
}
