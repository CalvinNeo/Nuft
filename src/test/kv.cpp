#include "node.h"
#include <vector>
#include <unistd.h>
#include <cstdlib>
#include <signal.h>
#include <cstdio>
#include <map>
#include <set>

void sigint_handler(){
    fflush(stdout);
    fflush(stderr);
    printf("Bye.\n");
    exit(0);
}

#define BUFFERSIZE 2048
char buffer[BUFFERSIZE];
char tmp[1024];

std::map<std::string, int> int_data;

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
        std::string cmd = buffer;
        if(cmd == "SET"){
            scanf("%s", buffer);
            std::string key = buffer;
            scanf("%s", buffer);
            std::string value = buffer;
            WaitApplied(node, "");

        }else if(cmd == "INCBY"){

        }else if(cmd == "GET"){
            scanf("%s", buffer);
            std::string key = buffer;
            node->do_log("");
        }
    }
}
