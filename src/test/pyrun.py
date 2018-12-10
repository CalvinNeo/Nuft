import subprocess
import os, sys, multiprocessing, time, signal
from threading import Thread, Timer
import threading

def wait_procs(procs, timeout):
    proc_size = len(procs)
    done_list = [False] * proc_size
    done_size = 0
    def kill_proc():
        for (i, proc) in enumerate(procs):
            if proc.poll() == None:
                # proc.terminate()
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    timer = Timer(timeout, kill_proc)
    timer.start()

    try:
        while True:
            for (i, proc) in enumerate(procs):
                if (not done_list[i]) and proc.poll() != None:
                    # If this proc is finished normally
                    done_size += 1
                    done_list[i] = True
            if done_size == proc_size:
                break
            time.sleep(0.1)

        # Necessary, otherwise the process will sleep until timer triggered
        timer.cancel()

    except KeyboardInterrupt:
        for proc in procs:
            if proc.poll() == None:
                # Give a chance to save work
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(0.2)
                # Note we can't add a `if` here, otherwise we can't eliminate all child procs.
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            timer.cancel()
        sys.exit(1)

def create_subproc(exe, fin, fout, ferr, in_shell = True):
    return subprocess.Popen(exe, stdin = fin, stdout = fout, stderr = ferr, shell = in_shell, preexec_fn = os.setsid, bufsize = 0)


