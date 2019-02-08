/*************************************************************************
*  Nuft -- A C++17 Raft consensus algorithm library
*  Copyright (C) 2018  Calvin Neo 
*  Email: calvinneo@calvinneo.com;calvinneo1995@gmail.com
*  Github: https://github.com/CalvinNeo/Nuft/
*  
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*  
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*  
*  You should have received a copy of the GNU General Public License
*  along with this program.  If not, see <https://www.gnu.org/licenses/>.
**************************************************************************/

#include "grpc_utils.h"
#include "node.h"
#include <iostream>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using RequestVoteResponse = ::raft_messages::RequestVoteResponse;
using RequestVoteRequest = ::raft_messages::RequestVoteRequest;
using AppendEntriesRequest = ::raft_messages::AppendEntriesRequest;
using AppendEntriesResponse = ::raft_messages::AppendEntriesResponse;
using InstallSnapshotRequest = ::raft_messages::InstallSnapshotRequest;
using InstallSnapshotResponse = ::raft_messages::InstallSnapshotResponse;
