import socket
import sys
import time
import struct
import threading
from _thread import *

HI = "%s - %s connected"
BYE = "%s - %s disconnected"
LOG = "%s %s %s"
HEADER_SZ = 4
LK = threading.Lock()
TIME = 100
bytes_received = [[0] for j in range(TIME)]
counter_dic = {}
counter = [[0] for j in range(TIME)]
current_slot = 0


def safe_print(lock, string):
    lock.acquire()
    print(string)
    lock.release()


def handle(sock, conn_t):
    global current_slot
    header = sock.recv(HEADER_SZ)
    if not header:
        return
    data_size = struct.unpack('i', header)[0]
    bt = sock.recv(data_size)
    if not bt:
        return
    node = bt.decode()
    safe_print(LK, HI % (conn_t, node))

    while True:
        header = sock.recv(HEADER_SZ)
        if not header:
            dsc_t = time.time()
            safe_print(LK, BYE % (dsc_t, node))
            break
        data_size = struct.unpack('i', header)[0]
        bt = sock.recv(data_size)
        if not bt:
            dsc_t = time.time()
            safe_print(LK, BYE % (dsc_t, node))
            break
        rct_t = time.time()
        current_slot = int(rct_t - conn_t)
        if current_slot > 99:
            with open('bandwidth.txt', 'w') as f:
                for row in bytes_received:
                    f.write(f'{row}\n')
            with open('delay.txt', 'w') as f:
                for row in counter:
                    f.write(f'{row}\n')
            dsc_t = time.time()
            safe_print(LK, BYE % (dsc_t, node))
            break
        msg = bt.decode()
        sd_t, log = msg.split()
        delay = rct_t - float(sd_t)
        if conn_t > 0:
            bytes_received[current_slot][0] += data_size
            if current_slot not in counter_dic:
                counter_dic[current_slot] = [delay]
                counter[current_slot] = [delay]
            else:
                counter_dic[current_slot].append(delay)
                counter[current_slot].append(delay)

        safe_print(LK, LOG % (sd_t, node, log))

    sock.close()


if len(sys.argv) > 1:
    port = int(sys.argv[1])
else:
    sys.exit(0)

sk = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)  # IPv4 TCP
sk.bind((socket.gethostbyname(socket.gethostname()), port))

sk.listen(10)
while True:
    s, addr = sk.accept()
    t = time.time()
    start_new_thread(handle, (s, t))
