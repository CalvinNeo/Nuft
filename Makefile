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

ifeq ($(DEBUG), test)
# @echo "debug mode"
LOGLEVEL = $(LOG_LEVEL_TEST)
else
# @echo "no debug mode"
LOGLEVEL = $(LOG_LEVEL_LIB)
endif

$(warning LOGLEVEL=$(LOGLEVEL))

CFLAGS += $(TRIM_WARN)
CFLAGS += $(CFLAGS_GRPC)

OBJ_EXT=o

ROOT = .
# Important not to include ".", or gcov -r will fail with some files
SRC_ROOT = src
BIN_ROOT = bin
OBJ_ROOT = $(BIN_ROOT)/obj
DYOBJ_ROOT = $(BIN_ROOT)/dyobj

SRCS = $(wildcard $(SRC_ROOT)/*.cpp)
SRCS_H = $(wildcard $(SRC_ROOT)/*.h)
OBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cpp, %o, $(SRCS)))
DYOBJS = $(patsubst $(SRC_ROOT)%, $(DYOBJ_ROOT)%, $(patsubst %cpp, %o, $(SRCS)))
GRPCSRCS_H = $(patsubst %proto, %pb.h, $(wildcard $(SRC_ROOT)/grpc/*.proto))
GRPCSRCS_H += $(patsubst %proto, %grpc.pb.h, $(wildcard $(SRC_ROOT)/grpc/*.proto))
GRPCSRCS_CPP = $(patsubst %proto, %pb.cc, $(wildcard $(SRC_ROOT)/grpc/*.proto))
GRPCSRCS_CPP += $(patsubst %proto, %grpc.pb.cc, $(wildcard $(SRC_ROOT)/grpc/*.proto))
GRPCCODES = $(GRPCSRCS_H) $(GRPCSRCS_CPP)
GRPCOBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cc, %o, $(GRPCSRCS_CPP)))
DYGRPCOBJS = $(patsubst $(SRC_ROOT)%, $(DYOBJ_ROOT)%, $(patsubst %cc, %o, $(GRPCSRCS_CPP)))

all: test

install: $(BIN_ROOT)/libnuft.so $(BIN_ROOT)/libnuft.a
	sudo cp $(BIN_ROOT)/libnuft.so /usr/local/lib/libnuft.so
	sudo cp $(BIN_ROOT)/libnuft.a /usr/local/lib/libnuft.a
	sudo mkdir -p /usr/local/include/Nuft/
	sudo mkdir -p /usr/local/include/Nuft/grpc
	sudo find $(SRC_ROOT) -name \*.h -exec cp {} /usr/local/include/Nuft \;
	sudo find $(SRC_ROOT)/grpc -name \*.h -exec cp {} /usr/local/include/Nuft/grpc \;
	sudo cp -r $(SRC_ROOT)/Nuke /usr/local/include/Nuke

uninstall:
	rm -rf /usr/local/include/Nuft/
	rm -rf /usr/local/include/Nuke/
	rm -rf usr/local/lib/libnuft.so
	rm -rf usr/local/lib/libnuft.a

test: $(GRPCOBJS) $(OBJS) $(SRC_ROOT)/test/test.cpp
	make test_pure DEBUG=test

test_pure: $(GRPCOBJS) $(OBJS) $(SRC_ROOT)/test/test.cpp
	$(CXX) $(CFLAGS) $(LOGLEVEL) $(SRC_ROOT)/test/test.cpp -o $(ROOT)/test -pthread $(GRPCOBJS) $(OBJS) /usr/local/lib/libgtest.a $(LDFLAGS)

atomic:
	cd $(SRC_ROOT)/test/atomic/ && make atomic DEBUG=normal

$(OBJ_ROOT)/grpc/%.o: $(SRC_ROOT)/grpc/%.cc $(GRPCCODES) $(OBJ_ROOT)/grpc 
	$(CXX) $(CFLAGS) $(LOGLEVEL) -pthread -I/usr/local/include -c -o $@ $< $(LDFLAGS)

$(OBJ_ROOT)/%.o: $(SRC_ROOT)/%.cpp $(GRPCCODES) $(OBJ_ROOT) 
	$(CXX) $(CFLAGS) $(LOGLEVEL) -pthread -I/usr/local/include -c -o $@ $< $(LDFLAGS)

$(DYOBJ_ROOT)/grpc/%.o: $(SRC_ROOT)/grpc/%.cc $(GRPCCODES) $(DYOBJ_ROOT)/grpc 
	$(CXX) $(CFLAGS) $(LOGLEVEL) -fPIC -pthread -I/usr/local/include -c -o $@ $< $(LDFLAGS)

$(DYOBJ_ROOT)/%.o: $(SRC_ROOT)/%.cpp $(GRPCCODES) $(DYOBJ_ROOT) 
	$(CXX) $(CFLAGS) $(LOGLEVEL) -fPIC -pthread -I/usr/local/include -c -o $@ $< $(LDFLAGS)

dbg:
	@echo $(OBJ_ROOT)/
	@echo $(OBJS)
	@echo $(GRPCCODES)

lib: $(BIN_ROOT)/libnuft.so $(BIN_ROOT)/libnuft.a

$(BIN_ROOT)/libnuft.so: $(DYOBJS) $(DYGRPCOBJS)
	$(CXX) $(CFLAGS) $(LOGLEVEL) -o $(BIN_ROOT)/libnuft.so -shared $(DYOBJS) $(DYGRPCOBJS) $(LDFLAGS) 

$(BIN_ROOT)/libnuft.a: $(OBJS) $(GRPCOBJS)
	ar rvs $(BIN_ROOT)/libnuft.a $(OBJS) $(GRPCOBJS)

$(GRPCCODES):
	protoc -I $(SRC_ROOT)/grpc --grpc_out=$(SRC_ROOT)/grpc --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $(SRC_ROOT)/grpc/raft_messages.proto
	protoc -I $(SRC_ROOT)/grpc --cpp_out=$(SRC_ROOT)/grpc $(SRC_ROOT)/grpc/raft_messages.proto

$(OBJ_ROOT)/grpc: $(OBJ_ROOT)
	mkdir -p $(OBJ_ROOT)/grpc

$(OBJ_ROOT):
	mkdir -p $(OBJ_ROOT)

$(DYOBJ_ROOT)/grpc: $(DYOBJ_ROOT)
	mkdir -p $(DYOBJ_ROOT)/grpc

$(DYOBJ_ROOT):
	mkdir -p $(DYOBJ_ROOT)

debug:
	export GRPC_TRACE=all
	export DEBUG=test

read_persist: $(GRPCOBJS)
	$(CXX) $(GRPCOBJS) $(CFLAGS) $(LDFLAGS) -Isrc/grpc $(SRCS) $(SRC_ROOT)/test/read_persist.cpp -o $(ROOT)/read_persist -lpthread /usr/local/lib/libgtest.a 

.PHONY: clean
clean: clc
	rm -rf $(BIN_ROOT)
	rm -f ./kv
	rm -rf ./test
	rm -rf ./read_persist
	rm -rf *.so
	rm -rf *.a
	cd $(SRC_ROOT)/test/atomic && make clean

.PHONY: clc
clc:
	rm -f *.err
	rm -f *.out
	rm -f *.persist
	rm -f .persist
	rm -f core
	
.PHONY: clean2
clean2: clean
	rm -rf $(SRC_ROOT)/grpc/*.pb.*

list:
	@echo $(SRCS)
	@echo $(OBJS)
	@echo $(GRPCSRCS)
	@echo $(GRPCOBJS)
