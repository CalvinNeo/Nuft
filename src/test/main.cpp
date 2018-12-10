#include "node.h"
#include "server.h"
#include <vector>
#include <unistd.h>
#include <cstdlib>
#include <signal.h>
#include <cstdio>

void sigint_handler(){
    fflush(stdout);
    fflush(stderr);
    exit(0);
}

#define BUFFERSIZE 2048
char buffer[BUFFERSIZE];
char tmp[1024];
int main(int argc, char *argv[]) {
    sigset(SIGINT, sigint_handler);
    sigset(SIGTERM, sigint_handler);

    int oc;
    int node_line = -1;
    std::vector<std::string> vec;
    while ((oc = getopt(argc, argv, "f:i:")) != -1)
    {
        switch (oc)
        {
        case 'f':
            sscanf(optarg, "%s", tmp);
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
    while(~scanf("%s", buffer)){
        printf("===\n");
        node->do_log(std::string(buffer));
    }
    node->raft_message_server->server->Wait();
}
