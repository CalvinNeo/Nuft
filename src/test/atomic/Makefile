HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CC = gcc
CXX = g++

cov_comp = -fprofile-arcs -ftest-coverage -fno-inline
cov_lnk = -fprofile-arcs -ftest-coverage --coverage -fno-inline

NO_WARN = -w
TRIM_WARN = -Wno-unused-variable -Wno-unused-but-set-variable -Wno-format-security -Wformat=0
GDB_INFO = -g
SANA = -fsanitize=address
SANT = -fsanitize=thread
CFLAGS = -DPOSIX -fpermissive -std=c++1z -L/usr/local/lib $(GDB_INFO) #$(SANT)

ifeq ($(SYSTEM),Darwin)
LDFLAGS += `pkg-config --libs protobuf grpc++ grpc` -lgrpc++_reflection -ldl 
else
LDFLAGS += `pkg-config --libs protobuf grpc++ grpc` -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl
endif
CFLAGS_GRPC = -DGRPC_VERBOSITY=DEBUG -DGRPC_TRACE=all
LOG_LEVEL_NOTICE_MAJOR = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_GRPC_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST
LOG_LEVEL_TEST = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST -D_HIDE_GRPC_NOTICE # -D_HIDE_RAFT_DEBUG
LOG_LEVEL_LIB = $(LOG_LEVEL_NOTICE_MAJOR) -D_HIDE_RAFT_DEBUG -D_HIDE_DEBUG -D_HIDE_TEST_DEBUG

OBJ_EXT=o

ROOT = ../../..
SRC_ROOT = $(ROOT)/src
BIN_ROOT = $(ROOT)/bin

GRPCSRCS_H = $(patsubst %proto, %pb.h, $(wildcard *.proto))
GRPCSRCS_H += $(patsubst %proto, %grpc.pb.h, $(wildcard *.proto))
GRPCSRCS_CPP = $(patsubst %proto, %pb.cc, $(wildcard *.proto))
GRPCSRCS_CPP += $(patsubst %proto, %grpc.pb.cc, $(wildcard *.proto))
GRPCCODES = $(GRPCSRCS_H) $(GRPCSRCS_CPP)
GRPCOBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cc, %o, $(GRPCSRCS_CPP)))

all: test

atomic: /usr/local/lib/libnuft.a $(GRPCCODES)
	$(CXX) $(CFLAGS) $(LOG_LEVEL_LIB) -I$(SRC_ROOT) atomic_server.cpp $(GRPCSRCS_CPP) -o $(BIN_ROOT)/atomic_server -pthread /usr/local/lib/libnuft.a $(LDFLAGS) 
	$(CXX) $(CFLAGS) $(LOG_LEVEL_LIB) -I$(SRC_ROOT) atomic_client.cpp $(GRPCSRCS_CPP) -o $(BIN_ROOT)/atomic_client -pthread /usr/local/lib/libnuft.a $(LDFLAGS) 

run_cluster:
	python run_cluster.py

$(GRPCCODES):
	protoc -I . --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` atomic.proto
	protoc -I . --cpp_out=. atomic.proto


.PHONY: clean
clean: clc
	rm -rf $(BIN_ROOT)
	rm -rf ./atomic*
	rm -rf *.o
	rm -rf *.pb.*
	rm -rf *.grpc.pb.*

.PHONY: clc
clc:
	rm -f core
	rm -f *.err
	rm -f *.out
