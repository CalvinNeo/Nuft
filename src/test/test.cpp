#include <gtest/gtest.h>
#include "../node.h"
#include "../server.h"
#include "../utils.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <future>
#include <tuple>

#define RN RaftNode::NodeState

uint16_t new_port_base = 7200;
uint16_t port_base = 7100;
std::vector<RaftNode *> nodes;
struct LogMon{
    std::string data;
    bool applied = false;
    int tid = 0;
};
std::map<int, LogMon> logs;
std::map<int, std::string> storage;
std::map<int, std::string> correct_storage;
std::mutex monitor_mut;

std::chrono::duration<int, std::milli> TimeEnsureElection(){
    std::chrono::duration<int, std::milli> ms{default_timeout_interval_lowerbound};
    return ms;
}
std::chrono::duration<int, std::milli> TimeEnsureSuccessfulElection(){
    std::chrono::duration<int, std::milli> ms{(default_timeout_interval_upperbound + default_election_fail_timeout_interval) * 3};
    return ms;
}
std::chrono::duration<int, std::milli> TimeEnsureNoElection(){
    std::chrono::duration<int, std::milli> ms{default_timeout_interval_lowerbound / 2};
    return ms;
}


void FreeRaftNodes(){
    // We move Leaders to the last of vector `nodes`, so we delete them at the end.
    // Before Leaders die, they will send their last AppendEntries RPC, which,
    // if handled without care, will interfere tests in the future.
    std::sort(nodes.begin(), nodes.end(), [](auto a, auto b){
        return a->state < b->state;
    });
    printf("=== Clearing %d\n", nodes.size());
    using namespace std::chrono_literals;
    for(auto nd: nodes){
        printf("=== Deleting %s\n", nd->name.c_str());
        delete nd;
        nd = nullptr;
    }
    nodes.clear();
    std::this_thread::sleep_for(1s);
}

void MakeRaftNodes(int n){
    // Must do cleaning first, because ASSERT will abort.
    FreeRaftNodes();
    nodes.resize(n);

    for(int i = 0; i < n; i++){
        nodes[i] = new RaftNode(std::string("127.0.0.1:") + std::to_string(port_base + i));
    }
    for(int i = 0; i < n; i++){
        for(int j = 0; j < n; j++){
            if(i == j) continue;
            nodes[i]->add_peer(nodes[j]->name);
        }
    }
    for(auto nd: nodes){
        nd->run();
    }
}

// RaftNode * MakeNewRaftNode(uint16_t port, const std::vector<std::string> & app, const std::vector<std::string> & rem){
RaftNode * AddRaftNode(uint16_t port){
    RaftNode * nd = new RaftNode(std::string("127.0.0.1:") + std::to_string(port));
    for(auto d: nodes){
        // Only set up newly added nodes.
        nd->add_peer(d->name);
    }
    nd->run();
    nodes.push_back(nd);
    return nd;
}

int CountLeader(){
    // TODO Is it necessary to take term into account?
    int tot = 0;
    for(auto nd: nodes){
        if(nd->state == RN::Leader && !nd->paused){
            tot ++;
        }
    }
    return tot;
}

RaftNode * PickNode(std::unordered_set<int> nt){
    // This function won't check if the are multiple Leader.
    for(auto nd: nodes){
        if(nt.find(nd->state) != nt.end() && !nd->paused){
            return nd;
        }
    }
    return nullptr;
}

int PickIndex(std::unordered_set<int> nt, int order = 0){
    // This function won't check if the are multiple Leader.
    for(int i = 0; i < nodes.size(); i++){
        RaftNode * nd = nodes[i];
        if(nt.find(nd->state) != nt.end() && !nd->paused){
            if(order){
                order--;
            }else{
                return i;
            }
        }
    }
    return -1;
}

void DisableNode(RaftNode * victim, bool unguarded = false){
    printf("GTEST: DisableNode %s\n", victim->name.c_str());
    if(unguarded){
        victim->stop_unguarded();
    }else{
        victim->stop();
    }
    for(auto nd: nodes){
        // Now all other nodes can't receive RPC from victim.
        // We don't disable send because we want to observe.
        nd->disable_receive(victim->name);
    }
}
void EnableNode(RaftNode * victim){
    victim->resume();
    for(auto nd: nodes){
        nd->enable_receive(victim->name);
    }
}
void DisableSend(RaftNode * victim, std::vector<int> nds){
    for(auto nd: nds){
        victim->disable_send(nodes[nd%nodes.size()]->name);
    }
}
void EnableSend(RaftNode * victim, std::vector<int> nds){
    for(auto nd: nds){
        victim->enable_send(nodes[nd%nodes.size()]->name);
    }
}
void DisableReceive(RaftNode * victim, std::vector<int> nds){
    for(auto nd: nds){
        victim->disable_receive(nodes[nd%nodes.size()]->name);
    }
}
void EnableReceive(RaftNode * victim, std::vector<int> nds){
    for(auto nd: nds){
        victim->enable_receive(nodes[nd%nodes.size()]->name);
    }
}
void CrashNode(RaftNode * victim){
    DisableNode(victim);
    // NOTICE In FrequentCrash test, CrashNode often causes segfault.
    printf("GTEST: Crash Node %s\n", victim->name.c_str());
    std::lock_guard<std::mutex> guard((victim->mut));
    victim->logs.clear();
    victim->current_term = 77777;
    victim->vote_for = "LLLLL";
    victim->name = victim->name;
    victim->reset_peers(guard);
    if(victim->trans_conf) delete victim->trans_conf;
    victim->trans_conf = nullptr;
    printf("GTEST: Crash Node %s FINISHED\n", victim->name.c_str());
}
void RecoverNode(RaftNode * victim){
    printf("GTEST: Recover Node %s\n", victim->name.c_str());
    victim->run(victim->name);
    EnableNode(victim);
}

template <typename F>
void WaitAny(RaftNode * ob, int ev, F f){
    std::mutex mut;
    std::condition_variable cv;

    ob->callbacks[ev] = [&mut, &cv, &f](int type, NuftCallbackArg * arg) -> int{
        if(f(type, arg)){
            std::unique_lock<std::mutex> lk(mut);
            cv.notify_one();
        }
        return 0;
    };
    std::unique_lock<std::mutex> lk(mut);
    cv.wait(lk);
    ob->callbacks[ev] = nullptr;
}
 
void WaitElection(RaftNode * ob){
    ob->debugging = 1;
    printf("GTEST: WaitElection ob %s\n", ob->name.c_str());
    WaitAny(ob, NUFT_CB_ELECTION_END, [&](int type, NuftCallbackArg * arg){
        if(Nuke::in(arg->a1, {RaftNode::ELE_SUC, RaftNode::ELE_FAIL, RaftNode::ELE_SUC_OB})){
            printf("GTEST: WaitElection Finish with %d.\n", arg->a1);
            return 1;
        }
        return 0;
    });
    ob->debugging = 0;
}

void WaitElectionStart(RaftNode * ob, std::unordered_set<int> ev){
    WaitAny(ob, NUFT_CB_ELECTION_START, [&](int type, NuftCallbackArg * arg){
        if(std::find(ev.begin(), ev.end(), arg->a1) != ev.end()){
            return 1;
        }
        return 0;
    });
}

void WaitConfig(RaftNode * ob, int ev, std::unordered_set<int> st){
    WaitAny(ob, ev, [&](int type, NuftCallbackArg * arg){
        if(std::find(st.begin(), st.end(), arg->a1) != st.end()){
            return 1;
        }
        return 0;
    });
}

int CheckCommit(IndexID index, const std::string & value){
    int support = 0;
    for(auto nd: nodes){
        ::raft_messages::LogEntry log;
        if((!nd->is_running()) || nd->get_log(index, log) != NUFT_OK){
            // Invalid node or no log found at index.
            if(!nd->is_running())
                printf("GTEST: In CheckCommit: %s is not running.\n", nd->name.c_str());
            if(nd->get_log(index, log) != NUFT_OK)
                printf("GTEST: In CheckCommit: %s have no %lld log.\n", nd->name.c_str(), index);
            continue;
        }
        TermID term = ~0;
        if(log.data() == value){
            if(term == ~0){
                term = log.term();
            }else if(term != log.term()){
                // Conflict at index
                printf("GTEST: In CheckCommit: Conflict at %lld, %s's term is %llu, term2 is %llu'.\n", index, nd->name.c_str(), log.term(), term);
                return -1;
            }
            if(nd->commit_index >= index){
                support++;
            }
        }else{
            // Conflict at index
            printf("GTEST: In CheckCommit: Conflict at %lld, %s is '%s', want '%s'.\n", index, nd->name.c_str(), log.data().c_str(), value.c_str());
            return -1;
        }
    }
    return support;
}

int CheckLog(IndexID index, const std::string & value){
    int support = 0;
    for(auto nd: nodes){
        ::raft_messages::LogEntry log;
        if((!nd->is_running()) || nd->get_log(index, log) != NUFT_OK){
            // Invalid node or no log found at index.
            if(!nd->is_running())
                printf("GTEST: CheckCommit: %s is not running.\n", nd->name.c_str());
            if(nd->get_log(index, log) != NUFT_OK)
                printf("GTEST: CheckCommit: %s have no %lld log.\n", nd->name.c_str(), index);
            continue;
        }
        TermID term = ~0;
        if(log.data() == value){
            if(term == ~0){
                term = log.term();
            }else if(term != log.term()){
                // Term conflict at index
                return -1;
            }
            support++;
        }else{
            // Data conflict at index
            return -1;
        }
    }
    return support;
}

void WaitApplied(RaftNode * ob, IndexID index){
    if(ob->last_applied >= index){
        return;
    }
    WaitAny(ob, NUFT_CB_ON_APPLY, [&](int type, NuftCallbackArg * arg){
        ApplyMessage * applymsg = (ApplyMessage *)(arg->p1);
        if(applymsg->index >= index){
            return 1;
        }
        return 0;
    });
}

template <typename F>
void MonitorApplied(RaftNode * ob, F f){
    // std::shared_ptr<std::mutex> pmut = std::make_shared<std::mutex>();
    ob->callbacks[NUFT_CB_ON_APPLY] = [f](int type, NuftCallbackArg * arg) -> int{
        std::lock_guard<std::mutex> guard((monitor_mut));
        ApplyMessage * applymsg = (ApplyMessage *)(arg->p1);
        assert(logs.find(applymsg->index) != logs.end());
        if(!logs[applymsg->index].applied){
            // NOTICE Must first set `applied`, because `f` will check if `applied == true` when doing snapshot.
            logs[applymsg->index].applied = true;
            f(arg, applymsg);
        }else{
            printf("GTEST: Monitor APPLIED %d\n", applymsg->index);
        }
        return NUFT_OK;
    };
}
void StopMonitorApplied(RaftNode * ob){
    ob->callbacks[NUFT_CB_ON_APPLY] = nullptr;
}

int RegisterLog(std::lock_guard<std::mutex> & guard, const std::string & s, RaftNode * leader, int log_id, int tid = 0){
    logs[log_id] = LogMon{s, false, tid};
    return log_id;
}
int RegisterLog(const std::string & s, RaftNode * leader, int log_id, int tid = 0){
    std::lock_guard<std::mutex> guard((monitor_mut));
    return RegisterLog(guard, s, leader, log_id, tid);
}

int MajorityCount(int n){
    return (n + 1) / 2;
}

void NetworkPartition(std::vector<int> p1, std::vector<int> p2){
    std::vector<std::string> sp1, sp2;
    std::transform(p1.begin(), p1.end(), std::back_inserter(sp1), [](int x){return std::to_string(x);});
    std::transform(p2.begin(), p2.end(), std::back_inserter(sp2), [](int x){return std::to_string(x);});
    printf("GTEST: NetworkPartition: part1 {%s}, part2 {%s}.\n", Nuke::join(sp1.begin(), sp1.end(), ",").c_str(), Nuke::join(sp2.begin(), sp2.end(), ",").c_str());
    for(auto pp1: p1){
        for(auto pp2: p2){
            nodes[pp1%nodes.size()]->disable_receive(nodes[pp2%nodes.size()]->name);
            nodes[pp2%nodes.size()]->disable_receive(nodes[pp1%nodes.size()]->name);
        }
    }
}

void RecoverNetworkPartition(std::vector<int> p1, std::vector<int> p2){
    printf("GTEST: RecoverNetworkPartition: part1 {%s}, part2 {%s}.\n", 
            Nuke::join(p1.begin(), p1.end(), ",", [](int x){return std::to_string(x);}).c_str(),
            Nuke::join(p2.begin(), p2.end(), ",", [](int x){return std::to_string(x);}).c_str());
    for(auto pp1: p1){
        for(auto pp2: p2){
            nodes[pp1]->enable_receive(nodes[pp2]->name);
            nodes[pp2]->enable_receive(nodes[pp1]->name);
        }
    }
}

void DisconnectAfterReplicateTo(RaftNode * leader, const std::string & log_str, std::unordered_set<int> nds){
    using namespace std::chrono_literals;
    std::unordered_set<int> mset;
    std::vector<int> sset;
    for(auto x: nds){
        mset.insert(x % nodes.size());
    }
    for(int i = 0; i < nodes.size(); i++){
        if(std::find(mset.begin(), mset.end(), i) == mset.end()){
            sset.push_back(i);
        }
    }
    printf("GTEST: In DisconnectAfterReplicateTo: Mute %s\n", Nuke::join(sset.begin(), sset.end(), ";", [](int x){return std::to_string(x);}).c_str());
    DisableSend(leader, sset);
    leader->do_log(log_str);
    // Make sure already sent to sset
    std::this_thread::sleep_for(TimeEnsureNoElection());
    DisableNode(leader);
}

int One(int index, int support){
    uint64_t now = get_current_ms();
    while(get_current_ms() < now + 5000){
        RaftNode * leader = PickNode({RN::Leader});
    }
}

void print_state(){
    printf("%15s %12s %5s %9s %7s %7s %7s %6s %6s\n",
            "Name", "State", "Term", "log size", "commit", "lastapp", "peers", "run", "trans");
    for(auto nd: nodes){
        printf("%15s %12s %5llu %9u %7lld %7lld %7u %6s %6d\n", nd->name.c_str(), 
                RaftNode::node_state_name(nd->state), nd->current_term, 
                nd->logs.size(), nd->commit_index, nd->last_applied, nd->peers.size(),
                nd->is_running()?"T":"F", (!nd->trans_conf)?0:nd->trans_conf->state);
    }
}

void print_peers(RaftNode * ob){
    printf("Node %s's peers are %s\n", ob->name.c_str(), Nuke::join(ob->peers.begin(), ob->peers.end(), ";", [](auto & pr){
        return pr.second->name;
    }).c_str());
}

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
    RaftNode * ob = PickNode({RN::Follower, RN::Candidate});
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

    int ln = 5;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        printf("GTEST[%d]: support %d\n", i, support);
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
        printf("GTEST[%d]: support %d, content %s\n", i, support, logstr.c_str());
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
    ASSERT_TRUE(leader != nullptr);
    
    int ln1 = 4, ln2 = 4;
    for(int i = 0; i < ln1; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        printf("GTEST[%d]: support %d\n", i, support);
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
        printf("GTEST[%d]: support %d\n", i, support);
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
    nodes[l1]->do_log("2");
    std::this_thread::sleep_for(TimeEnsureNoElection());
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
    std::this_thread::sleep_for(1s);
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
    int l1 = PickIndex({RN::Leader});
    ASSERT_NE(l1, -1);
    
    // (a)
    nodes[l1]->do_log("1");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(nodes[l1]->commit_index, 0);
    print_state();

    // S1 replicate successfully only to S2, then crashed.
    printf("GTEST: START Network Partition\n");
    DisconnectAfterReplicateTo(nodes[l1], "2", {l1+1});
    printf("GTEST: Crash\n");
    // Note leader is crashed now, we don't count its vote in `CheckLog` abd `CheckCommit`.
    print_state();
    ASSERT_EQ(CheckCommit(0, "1"), 4);
    ASSERT_EQ(CheckCommit(1, "2"), 0);
    // Only S2 has entry "2"
    ASSERT_EQ(CheckLog(1, "2"), 1);
    ASSERT_EQ(nodes[l1]->commit_index, 0);
    
    // (b)
    // S2-S5 timeout.
    WaitElection(nodes[(l1+2)%n]);
    std::this_thread::sleep_for(1s);
    // Note S1 crashed. so return 1 rather than 2.
    ASSERT_EQ(CountLeader(), 1);
    int l2 = PickIndex({RN::Leader}, 0);
    ASSERT_NE(l1%n, l2%n);
    printf("GTEST: Old Leader %d, New Leader %d\n", l1, l2);
    print_state();
    // S2's wrong entry (term 2, index 2) was not removed before we DisableSend from l2 to l1+1.
    DisconnectAfterReplicateTo(nodes[l2], "3", {});
    print_state();
    if(l2 == l1 + 1){
        ASSERT_EQ(CheckCommit(2, "3"), 0);
    }else{
        // nodes[l1+1]'s log entry "2" is overwritten by nodes[l2]
        ASSERT_EQ(CheckCommit(1, "3"), 0);
    }
//    printf("GTEST: Recover S5.\n");
//    EnableNode(nodes[l2]);
//    WaitElection(nodes[l2]);
//    std::this_thread::sleep_for(1s);
//    ASSERT_EQ(CheckCommit(1, "3"), 0);
    // (c)
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
    WaitConfig(leader, NUFT_CB_CONF_END, {RN::Leader, RN::Follower});
    printf("GTEST: Now switched to new conf.\n");
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
    WaitAny(leader, NUFT_CB_CONF_START, [&](int type, NuftCallbackArg * args){
            if(args->a1 == RaftNode::Configuration::State::JOINT){
                // If we crash at JOINT, we must ensure the new leader will resume the rest stages of conf trans.
                // Now joint consensus is committed, but we Disable Leader.
                printf("GTEST: Joint consensus committed, disable Leader Again.\n");
                // NOTICE the lambda holds the lock of raft node.
                print_state();
                DisableNode(leader, true);
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

    int ln = 4;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        printf("GTEST: support %d\n", support);
        // Shutting one node will not affect.
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
    leader->do_log("COMMIT");
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(CheckCommit(ln, "COMMIT"), n);
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
    WaitAny(leader, NUFT_CB_CONF_START, [&](int type, NuftCallbackArg * args){
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
        leader->do_log(std::to_string(i) + ";");
        if(i % crash_interval == 1){
            WaitApplied(leader, i);
            for(auto nd: nodes) {
                CrashNode(nd);
            }
            print_state();
            for(auto nd: nodes) {
                RecoverNode(nd);
            }
            print_state();
            std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
            leader = PickNode({RN::Leader});
            ASSERT_TRUE(leader != nullptr);
        }
    }
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
        MonitorApplied(nd, [snap_interval](NuftCallbackArg * arg, ApplyMessage * applymsg){
            IndexID index = applymsg->index;
            if(applymsg->from_snapshot){
                printf("GTEST: Apply snapshot logs[%d] = '%s'\n", index, applymsg->data.c_str());
                storage[0] = applymsg->data;
            }else{
                printf("GTEST: Apply logs[%d] = '%s'\n", index, applymsg->data.c_str());
                std::vector<std::string> l = Nuke::split(applymsg->data, "=");
                int tid = atoi(l[0].c_str());
                if(!Nuke::contains(storage, tid)){
                    storage[tid] = "";
                }
                storage[tid] += applymsg->data;
                if(logs[index].data != applymsg->data){
                    printf("GTEST: logs[%d].data != applymsg->data\n", index);
                }
                ASSERT_EQ(logs[index].data, applymsg->data);
                printf("GTEST: storage = '%s'\n", storage[tid].c_str());
                if(snap_interval > 0 && index % snap_interval == snap_interval - 1){
                    // Move to MonitorApply, Under protection from `monitor_mut`.
                    // NOTICE deadlock may happend here, because `do_apply` may hold lock.
                    printf("GTEST: APPLIED of %d is %d. Now do snapshot\n", index, logs[index].applied);
                    print_state();
                    ASSERT_EQ(logs[index].applied, true);
                    arg->node->do_install_snapshot_unguard(index, storage[tid]);
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
    auto inner_loop = [&](){
        int tid = current_tid.fetch_add(1);
        for(int i = 0; i < tot; i++){
            std::string s = std::to_string(tid) + "=" + std::to_string(i) + ";";
            // NOTICE There may be so many tasks in `task_queue`,
            // that we can't send heartbeat immediately.
            // Then one of our nodes timeout and start an election.
            int retry = 0;
            while(!CountLeader() == 1){
                std::this_thread::sleep_for(TimeEnsureElection());
                retry++;
                ASSERT_LT(retry, 4);
            }
            RaftNode * leader = PickNode({RN::Leader});
            // NOTICE We must use this callback to RegisterLog.
            // Otherwise there's a slight chance that RaftNode will apply before we RegisterLog. 
            int log_id;
            while(1){
                auto cb = [&](RaftNode * nd) -> int{
                    // TODO NOTICE Can't move `do_log` into `RegisterLog`, Otherwise can cause deadlock.
                    IndexID last_log_index = nd->last_log_index();
                    assert(nd->logs.back().data() == s);
                    RegisterLog(s, nd, last_log_index, tid);
                    return 0;
                };
                if(leader){
                    log_id = leader->do_log(s, cb);
                }
                if(log_id < 0){
                    printf("GTEST: Do Log Error %d\n", log_id);
                    std::this_thread::sleep_for(TimeEnsureElection());
                    leader = PickNode({RN::Leader});
                }else{
                    break;
                }
            }
            printf("GTEST: Do logs[%d] from thread %d = '%s'\n", log_id, tid, s.c_str());
            assert(logs.find(log_id) != logs.end());
            // printf("GTEST: Do Log %d Finish\n", i);
            if(!Nuke::contains(correct_storage, tid)){
                correct_storage[tid] = "";
            }
            correct_storage[tid] += s;
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
    // Give enought time.
    std::this_thread::sleep_for(TimeEnsureSuccessfulElection());
    RaftNode * leader = PickNode({RN::Leader});
    // Prevent "can't commit logs replicated by previous Leaders".
    printf("GTEST: Whip at %d\n", tot * clients);
    RegisterLog("0=Whip", leader, tot * clients, 0);
    leader->do_log("0=Whip");
    correct_storage[0] += "0=Whip";
    std::this_thread::sleep_for(std::chrono::duration<int, std::milli>{35 * tot * clients});
    delete [] ths;
    print_state();
    for(int i = 0; i < clients; i++){
        printf("GTEST: storage[%d] is '%s'\n", i, storage[i].c_str());
    }
    leader = PickNode({RN::Leader});
    ASSERT_NE(leader, nullptr);
    ASSERT_EQ(leader->commit_index, tot * clients);
    ASSERT_EQ(leader->last_applied, tot * clients);
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
    GenericTest(100, 5, 30, true, false, 1);
}

TEST(Snapshot, LostAndCrash){
    GenericTest(100, 5, 30, true, true, 1);
}

TEST(Concurrent, Basic){
    GenericTest(50, 5, -1, false, false, 3);
}

// TEST(Concurrent, LostAndCrash){
//     GenericTest(50, 5, -1, true, true, 3);
// }

int main(int argc, char ** argv){
    testing::InitGoogleTest(&argc, argv);
    auto r = RUN_ALL_TESTS();
    FreeRaftNodes();
    return r;
}
