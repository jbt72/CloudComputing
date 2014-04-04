import sys
import socket
import datetime

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 8765
toaddr = sys.argv[3] if len(sys.argv) > 3 else "nobody@example.com"
fromaddr = sys.argv[4] if len(sys.argv) > 4 else "nobody@example.com"
username = "Johanni27"
password = "1234"


def send(socket, message):
    # In Python 3, must convert message to bytes explicitly.
    # In Python 2, this does not affect the message.
    socket.send(message.encode('utf-8'))

def sendmsg(msgid, hostname, portnum, sender, receiver):
    # make outgoing connection
    #AF_INET is the address family that is used for the socket you're creating (in this case an Internet Protocol address) IPv4
    # Connection based stream (TCP)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, portnum))

    # transmit data
    # send() returns actual bytes sent
    print(s.recv(500))
    print("socket hostname %s\r\n" % socket.gethostname())
    send(s, "%s\r\n" % username)
    send(s, "%s\r\n" % password)
    # identity has been validated
    send(s, "GET %s\r\n" % "mykey")
    print(s.recv(500))
    send(s, "SET %s %s\r\n" % ("mykey", "Hello"))
    print(s.recv(500))

    print(s.recv(500))

sendmsg(1, host, port, fromaddr, toaddr)
