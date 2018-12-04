protoc -I . --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` raft_messages.proto
protoc -I . --cpp_out=. raft_messages.proto
