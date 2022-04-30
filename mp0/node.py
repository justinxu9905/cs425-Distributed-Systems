import socket
import sys
import struct

if len(sys.argv) > 3:
    name = sys.argv[1]
    addr = sys.argv[2]
    port = int(sys.argv[3])
else:
    sys.exit(0)

sk = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
sk.connect((addr, port))
bt_name = str(name).encode('utf-8')
sk.send(struct.pack('i', len(bt_name)))
sk.send(bt_name)

while True:
    msg = input()
    bt_msg = msg.encode('utf-8')
    sk.send(struct.pack('i', len(bt_msg)))
    sk.send(bt_msg)
