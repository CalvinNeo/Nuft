#include <gtest/gtest.h>
#include "../node.h"
#include "../server.h"
#include "../utils.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>

uint16_t port_base = 7100;
std::vector<RaftNode *> nodes;

void FreeRaftNodes(){
    printf("=== Clearing %d\n", nodes.size());
    for(auto nd: nodes){
        printf("=== Deleting %s\n", nd->name.c_str());
        delete nd;
    }
    nodes.clear();
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
        nd->start();
    }
}

int CountLeader(){
    // TODO Is it necessary to take term into account?
    int tot = 0;
    for(auto nd: nodes){
        if(nd->state == RaftNode::NodeState::Leader && !nd->paused){
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

int DisableNode(RaftNode * victim){
    victim->stop();
    for(auto nd: nodes){
        nd->ignore_peer(victim->name);
    }
}
int EnableNode(RaftNode * victim){
    victim->resume();
    for(auto nd: nodes){
        nd->stop_ignore_peer(victim->name);
    }
}

void WaitElection(RaftNode * major){
    major->debugging = 1;
    std::mutex mut;
    std::condition_variable cv;
    printf("GTEST WaitElection ob %s\n", major->name.c_str());
    major->callbacks[NUFT_CB_ELECTION_END] = [&mut, &cv](int type, NuftCallbackArg * arg) -> int{
        if(arg->a1 == RaftNode::ELE_SUC || arg->a1 == RaftNode::ELE_FAIL || arg->a1 == RaftNode::ELE_SUC_OB){
            std::unique_lock<std::mutex> lk(mut);
            cv.notify_one();
        }
        return 0;
    };
    std::unique_lock<std::mutex> lk(mut);
    cv.wait(lk);
	major->callbacks[NUFT_CB_ELECTION_END] = nullptr;
    major->debugging = 0;
}

void WaitElectionStart(RaftNode * major, std::unordered_set<int> ev){
    std::mutex mut;
    std::condition_variable cv;

    major->callbacks[NUFT_CB_ELECTION_START] = [&mut, &cv, &ev](int type, NuftCallbackArg * arg) -> int{
        for(auto e: ev){
            if(arg->a1 == e){
                goto HIT;
            }
        }
        return 0;
HIT:
        std::unique_lock<std::mutex> lk(mut);
        cv.notify_one();
        return 0;
    };
    std::unique_lock<std::mutex> lk(mut);
    cv.wait(lk);
    major->callbacks[NUFT_CB_ELECTION_START] = nullptr;
}

int CheckCommit(IndexID index, const std::string & value){
    int support = 0;
    for(auto nd: nodes){
        ::raft_messages::LogEntry log;
        if((!nd->is_running()) || nd->get_log(index, log) != NUFT_OK){
            // Invalid node or no log found at index.
            if(!nd->is_running())
            printf("CheckCommit: %s is not running.\n", nd->name.c_str());
            if(nd->get_log(index, log) != NUFT_OK)
            printf("CheckCommit: %s have no %lld log.\n", nd->name.c_str(), index);
            continue;
        }
        TermID term = ~0;
        if(log.data() == value){
            if(term == ~0){
                term = log.term();
            }else if(term != log.term()){
                // Conflict at index
                return -1;
            }
            if(nd->commit_index >= index){
                support++;
            }
        }else{
            // Conflict at index
            return -1;
        }
    }
    return support;
}

int MajorityCount(int n){
    return (n + 1) / 2;
}

TEST(Network, RedirectToLeader){
    int n = 5;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
	RaftNode * ob = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
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
    int n = 5;
    ASSERT_GT(n, 1);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    // It is important to wait for a while and let the election ends in all node.
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
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
    RaftNode * victim = PickNode({RaftNode::NodeState::Leader});
    ASSERT_TRUE(victim != nullptr);
    DisableNode(victim); // We close leader and observe another node.
    // Can't wait here, or a new Leader will be elected. 
    ASSERT_EQ(CountLeader(), 0);
	
    // Election
    RaftNode * ob = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
    WaitElectionStart(ob, {RaftNode::ELE_VOTE, RaftNode::ELE_NEW, RaftNode::ELE_AGA});
    printf("GTEST: Observe election through %s\n", ob->name.c_str());
	WaitElection(ob);
    std::this_thread::sleep_for(1s);
    RaftNode * new_leader = PickNode({RaftNode::NodeState::Leader});
    ASSERT_EQ(CountLeader(), 1);

    // Now old Leader came back
    EnableNode(victim);
    std::this_thread::sleep_for(2s);
    ASSERT_EQ(PickNode({RaftNode::NodeState::Leader}), new_leader);
    FreeRaftNodes();
}

TEST(Commit, Normal){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * leader = PickNode({RaftNode::NodeState::Leader});
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
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * leader = PickNode({RaftNode::NodeState::Leader});
    ASSERT_TRUE(leader != nullptr);
    
    // Disable 1 node
    RaftNode * victim = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
    ASSERT_NE(victim, nullptr);
    DisableNode(victim);

	int ln = 4;
    for(int i = 0; i < ln; i++){
        std::string logstr = std::string("log")+std::to_string(i);
        leader->do_log(logstr);
        std::this_thread::sleep_for(1s);
        int support = CheckCommit(i, logstr);
        printf("GTEST: support %d\n", support);
        ASSERT_GE(support, MajorityCount(n));
    }

    // Disable 2 nodes
	RaftNode * victim2 = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
	DisableNode(victim2);
	leader->do_log("DOOMED");
    std::this_thread::sleep_for(1s);
	ASSERT_EQ(CheckCommit(ln, "DOOMED"), 0);
	ASSERT_EQ(leader->commit_index, ln - 1);

    // Enable victim2
    EnableNode(victim2);
    // According to strange_failure5.log, 2s seems not enough.
    std::this_thread::sleep_for(5s);
    ASSERT_GE(CheckCommit(ln, "DOOMED"), MajorityCount(n));
    ASSERT_EQ(leader->commit_index, ln);
	FreeRaftNodes();
}

TEST(Commit, LeaderLost){
    int n = 3;
    ASSERT_GT(n, 2);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * leader = PickNode({RaftNode::NodeState::Leader});
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
    RaftNode * ob = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
    WaitElection(ob);

    std::this_thread::sleep_for(1s);
    RaftNode * new_leader = PickNode({RaftNode::NodeState::Leader});
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

int main(int argc, char ** argv){
    testing::InitGoogleTest(&argc, argv);
    auto r = RUN_ALL_TESTS();
    FreeRaftNodes();
    return r;
}
