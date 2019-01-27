#include "../node.h"
#include "../grpc_utils.h"
#include <fstream>

using namespace std;

int main(int argc, char * argv[]){
    if(argc <= 1){
        printf("Please input persist node name.\n");
        return 0;
    }
    std::fstream fo{(argv[1] + std::string(".persist")).c_str(), std::ios::binary | std::ios::in};
    raft_messages::PersistRecord record;
    record.ParseFromIstream(&fo);
    printf("current_term: %d\n", record.term());
    printf("name: %s\n", record.name().c_str());
    printf("vote_for: %s\n", record.vote_for().c_str());
    printf("current_term: %d\n", record.term());
    for(int i = 0; i < record.entries_size(); i++){
        printf("logs[%d]: term = %llu, data = '%s'\n", i, record.entries(i).term(), record.entries(i).data().c_str());
    }

    if(record.has_conf_record()){
        const raft_messages::ConfRecord & confrecord = record.conf_record();
        RaftNode::Configuration conf = RaftNode::Configuration::from_string(confrecord.peers());
        printf("conf.peers = '%s'\n", confrecord.peers().c_str());
    }
    fo.close();

}