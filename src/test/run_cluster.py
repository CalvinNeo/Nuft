from pyrun import *
import subprocess, os

def main():
    print os.getcwd()
    procs = []
    procs += [create_subproc("./bin/node -f./settings.txt -i0", subprocess.PIPE, open("node0.out", "w"), open("node0.err", "w"), 1)]
    procs += [create_subproc("./bin/node -f./settings.txt -i1", subprocess.PIPE, open("node1.out", "w"), open("node1.err", "w"), 1)]
    wait_procs(procs, 100.0)

if __name__ == '__main__':
    main()
