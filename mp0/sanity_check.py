import os
import subprocess
import time
import sys
import signal
from hashlib import sha256
import psutil


def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()

rstring = sha256(os.urandom(20)).hexdigest()
event = f'{time.time()} {rstring}\n'.encode()

## check for logger and node executable in curdir
if not os.path.exists("node") or not os.access("node", os.X_OK):
    print("No node executable found")
    sys.exit(1)

if not os.path.exists("logger") or not os.access("logger", os.X_OK):
    print("No logger executable found")
    sys.exit(1)


# run logger and node on port 2222
try:
    logger_p = subprocess.Popen(["./logger", "2222"], stdout=subprocess.PIPE)
except:
    print("Unable to execute logger process")
    sys.exit(1)

# wait for logger to spin up before launching node
time.sleep(1)

try:
    node_p = subprocess.Popen(["./node", "test", "127.0.0.1", "2222"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
except:
    print("Unable to execute node process")
    kill(logger_p.pid)
    sys.exit(1)

try:
    node_p.communicate(event, timeout=1)
except:
    pass

try:
    kill(node_p.pid)
except:
    pass

time.sleep(1)

try:
    kill(logger_p.pid)
except:
    pass

# check connect + disconnect message in logger
stdout, stderr = logger_p.communicate()
stdout = stdout.decode()

print(stdout)

if stdout.find("test connected") >= 0 and stdout.find("test disconnected") >= 0 and stdout.find(rstring):
    print("Sanity Check: Passed!")
else:
    print("Output incorrect")