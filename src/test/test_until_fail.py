from pyrun import *
import subprocess, os

def main():
    print os.getcwd()
    i = 0
    while 1:
    	print "Start test -- {}".format(i)
        f = open("out.log", "w")
        returncode = subprocess.call("./test", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        # returncode = subprocess.call("./test --gtest_filter='Election.Normal'", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        # returncode = subprocess.call("./test --gtest_filter='Persist.FrequentCrash'", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        # returncode = subprocess.call("./test --gtest_filter='Persist.AddPeer'", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        # returncode = subprocess.call("./test --gtest_filter='Config.DelLeader'", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        # returncode = subprocess.call("./test --gtest_filter='Commit.LeaderChange'", stdin = subprocess.PIPE, stdout = f, stderr = f, shell=True)
        if returncode != 0:
        	print "Test err."
        	return
        i += 1

if __name__ == '__main__':
    main()
