# Nuft
Nuft is an C++17 implementation of the Raft protocol.

# Build
## Requirement
1. C++17 standard(e.g. g++ 7.2)
2. gRPC
3. [Nuke](https://github.com/CalvinNeo/Nuke), which is already packed in source
4. gtest(in /usr/local/lib)

## Build tests
Build all tests by
```
make
```
Run all tests by
```
./test
```
# Usage
## API 
1. `NuftResult RaftNode::do_log(const std::string & log_string)`
2. `NuftResult RaftNode::do_log(raft_messages::LogEntry entry, int command = 0)`
3. `void RaftNode::run()`
4. `void RaftNode::run(const std::string & new_name)`
5. `void RaftNode::stop()`
6. `void RaftNode::resume()`
7. `NuftResult RaftNode::do_install_snapshot(IndexID last_included_index, const std::string & state_machine_state)`
8. `NuftResult RaftNode::update_configuration(const std::vector<std::string> & app, const std::vector<std::string> & rem)`
9. `std::string RaftNode::get_leader_name() const`
10. `void safe_leave()`

## Callbacks

# License

    Nuft -- A C++17 Raft consensus algorithm library
    Copyright (C) 2018  Calvin Neo 
    Email: calvinneo@calvinneo.com;calvinneo1995@gmail.com
    Github: https://github.com/CalvinNeo/Nuft/

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
