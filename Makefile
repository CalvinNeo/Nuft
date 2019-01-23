HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CC = gcc
CXX = g++

cov_comp = -fprofile-arcs -ftest-coverage -fno-inline
cov_lnk = -fprofile-arcs -ftest-coverage --coverage -fno-inline

NO_WARN = -w
TRIM_WARN = -Wno-unused-variable -Wno-unused-but-set-variable -Wformat-security
GDB_INFO = -g
CFLAGS = -DPOSIX -fpermissive -std=c++1z -L/usr/local/lib $(GDB_INFO)

GRPC_PKGCONFIG = `pkg-config --libs protobuf grpc++ grpc` 
ifeq ($(SYSTEM),Darwin)
LDFLAGS += -DGRPC_VERBOSITY=DEBUG -DGRPC_TRACE=all $(GRPC_PKGCONFIG) -lgrpc++_reflection -ldl 
else
LDFLAGS += -DGRPC_VERBOSITY=DEBUG -DGRPC_TRACE=all $(GRPC_PKGCONFIG) -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl
endif

LOG_LEVEL_NOTICE_MAJOR = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_GRPC_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST
LOG_LEVEL = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST -D_HIDE_GRPC_NOTICE 
CFLAGS += $(LOG_LEVEL)

OBJ_EXT=o

ROOT = .
# Important not to include ".", or gcov -r will fail with some files
SRC_ROOT = src
BIN_ROOT = bin
OBJ_ROOT = $(BIN_ROOT)/obj

SRCS = $(wildcard $(SRC_ROOT)/*.cpp)
OBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cpp, %o, $(SRCS)))
GRPCSRCS = $(wildcard $(SRC_ROOT)/grpc/*.cc)
GRPCOBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cc, %o, $(GRPCSRCS)))

all: 
	make grpc_source
	make test

test: $(GRPCOBJS)
	$(CXX) $(GRPCOBJS) $(CFLAGS) $(LDFLAGS) -Isrc/grpc $(SRCS) $(SRC_ROOT)/test/test.cpp -o $(ROOT)/test -lpthread /usr/local/lib/libgtest.a 

run_cluster:
	python $(SRC_ROOT)/test/run_cluster.py

node: $(GRPCOBJS)
	$(CXX) $(GRPCOBJS) $(CFLAGS) $(LDFLAGS) -Isrc/grpc $(SRCS) $(SRC_ROOT)/test/test.cpp -o $(BIN_ROOT)/node

$(OBJ_ROOT)/%.o: $(SRC_ROOT)/%.cc $(OBJ_ROOT)/grpc 
	$(CXX) $(CFLAGS) -pthread -I/usr/local/include -c -o $@ $<

grpc_source:
	protoc -I $(SRC_ROOT)/grpc --grpc_out=$(SRC_ROOT)/grpc --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $(SRC_ROOT)/grpc/raft_messages.proto
	protoc -I $(SRC_ROOT)/grpc --cpp_out=$(SRC_ROOT)/grpc $(SRC_ROOT)/grpc/raft_messages.proto

$(OBJ_ROOT)/grpc: $(OBJ_ROOT)
	mkdir -p $(OBJ_ROOT)/grpc

$(OBJ_ROOT):
	mkdir -p $(OBJ_ROOT)

debug:
	export GRPC_TRACE=all

.PHONY: clean
clean: clc
	rm -rf $(BIN_ROOT)
	rm -f core
	rm -rf ./test

.PHONY: clc
clc:
	rm -f *.err
	rm -f *.out
	rm -f *.persist
	
.PHONY: clean2
clean2: clean
	rm -rf $(SRC_ROOT)/grpc/*.pb.*
	

list:
	@echo $(SRCS)
	@echo $(OBJS)
	@echo $(GRPCSRCS)
	@echo $(GRPCOBJS)
