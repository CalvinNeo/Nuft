#include "node.h"
#include "server.h"

RaftNode * make_raft_node(const std::string & addr) {
    RaftNode * node = new RaftNode(addr);
    return node;
}
