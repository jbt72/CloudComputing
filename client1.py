import sys
import socket
import datetime

#hostname and port for load balancer
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

def setup_connection(hostname, portnum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, portnum))
    print(s.recv(500))
    send(s, "CONNECT\r\n")
    response = s.recv(500) # will be the hostname and portnum of assigned server
    print(response)
    return response.split(" ")[0:2]


def sendmsg(msgid, hostname, portnum, sender, receiver):
    # make outgoing connection
    #AF_INET is the address family that is used for the socket you're creating (in this case an Internet Protocol address) IPv4
    # Connection based stream (TCP)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname_s, int(portnum_s)))

    # transmit data
    # send() returns actual bytes sent
    print(s.recv(500))
    print("socket hostname %s" % socket.gethostname())
    print("username: %s password: %s" % (username, password))
    send(s, "%s\r\n" % username)
    send(s, "%s\r\n" % password)
    print(s.recv(500))
    # identity has been validated
    print("GET %s" % "mykey")
    send(s, "GET %s\r\n" % "mykey")
    print(s.recv(500))
    print("SET %s %s" % ("mykey1", "Hello1"))
    send(s, "SET %s %s\r\n" % ("mykey1", "Hello1"))
    print(s.recv(500))
    print("SET %s %s" % ("mykey2", "Hello2"))
    send(s, "SET %s %s\r\n" % ("mykey2", "Hello2"))
    print(s.recv(500))
    print("SET %s %s" % ("mykey3", "Hello3"))
    send(s, "SET %s %s\r\n" % ("mykey3", "Hello3"))
    print(s.recv(500))
    print("DEL %s %s %s" % ("mykey1", "mykey2", "mykey3"))
    send(s, "DEL %s %s %s\r\n" % ("mykey1", "mykey2", "mykey3"))
    print(s.recv(500))
    print("QUIT")
    send(s, "QUIT\r\n")
    print(s.recv(500))


# connect to load balancer and get a hostname and portnum for server
(hostname_s, portnum_s) = setup_connection(host, port)
sendmsg(1, hostname_s, portnum_s, fromaddr, toaddr)
