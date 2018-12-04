#include "node.h"
#include "server.h"
#include <vector>
#include <unistd.h>
#include <cstdlib>

char tmp[1024];
int main(int argc, char *argv[]) {
    int oc;
    int node_line = -1;
    std::vector<std::string> vec;
    while ((oc = getopt(argc, argv, "f:i:")) != -1)
    {
        switch (oc)
        {
        case 'f':
            sscanf(optarg, "%s", &tmp);
            break;
        case 'i':
            sscanf(optarg, "%d", &node_line);
            break;
        }
    }
    if (node_line == -1) {
        return 0;
    }
    FILE * fin = fopen(tmp, "r");
    while (fgets(tmp, 1024, fin) != NULL)
    {
        std::string saddr = tmp;
        Nuke::trim(saddr);
        vec.push_back(saddr);
    }
    printf("Create node at %s\n", vec[node_line].c_str());
    RaftNode * node = make_raft_node(vec[node_line]);
    for (int i = 0; i < vec.size(); ++i)
    {
        if (i != node_line) {
            printf("Add peer %s\n", vec[i].c_str());
            node->add_peer(vec[i]);
        }
    }
    node->start();
    node->raft_message_server->server->Wait();
}