import sys
sys.path.append("..")

from pyrun import *
import subprocess, os

def run_cluster(nodes):
    procs = []
    for i in xrange(len(nodes)):
        name = nodes[i]
        peers = ""
        for j in xrange(len(nodes)):
            if i != j:
                peers += nodes[j] + ";"
        X = "./bin/atomic_server -n\"{}\" -p\"{}\"".format(name, peers)
        print "Start {}".format(X)
        procs += [create_subproc(X, None, open("node{}.out".format(i), "w"), open("node{}.err".format(i), "w"), 1)]
    wait_procs(procs, 100.0)

def main():
    print os.getcwd()
    ports = [7101, 7102, 7103]
    nodes = map(lambda p: "127.0.0.1:" + str(p), ports)
    return run_cluster(nodes)

if __name__ == '__main__':
    main()
