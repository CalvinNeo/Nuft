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
    for(auto nd: nodes){
        delete nd;
    }
    nodes.clear();
}

void MakeRaftNodes(int n){
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

void MakeLog(){
    
}

int CountLeader(){
    int tot = 0;
    for(auto nd: nodes){
        if(nd->state == RaftNode::NodeState::Leader){
            tot ++;
        }
    }
    return tot;
}

RaftNode * PickNode(std::unordered_set<int> nt){
    // This function won't check if the are multiple Leader.
    for(auto nd: nodes){
        if(nt.find(nd->state) != nt.end()){
            return nd;
        }
    }
    return nullptr;
}

int DisableNode(RaftNode * major){
    major->stop();
    for(auto nd: nodes){
        nd->ignore_peer(major->name);
    }
}

void WaitElection(RaftNode * major){
    major->debugging = 1;
    std::mutex mut;
    std::condition_variable cv;
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

TEST(Election, ReelectionAfterFailure){
    int n = 4;
    ASSERT_GT(n, 1);
    MakeRaftNodes(n);
    WaitElection(nodes[0]);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    RaftNode * victim = PickNode({RaftNode::NodeState::Leader});
    ASSERT_TRUE(victim != nullptr);
    DisableNode(victim); // We close leader and observe major.
    ASSERT_EQ(CountLeader(), 0);
	RaftNode * major = PickNode({RaftNode::NodeState::Follower, RaftNode::NodeState::Candidate});
    WaitElectionStart(major, {RaftNode::ELE_VOTE, RaftNode::ELE_NEW, RaftNode::ELE_AGA});
    printf("GTEST: Observe election through %s\n", major->name.c_str());
	WaitElection(major);
	ASSERT_EQ(CountLeader(), 1);
    FreeRaftNodes();
}

int main(int argc, char ** argv){
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
