import sys
import socket
import datetime

#hostname and port for load balancer
host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 8790

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


    print("CREATE ALBUM \t%s\t%s" % ("Johanni27", "album1J"))
    send(s, "CREATE ALBUM \t%s\t%s\r\n" % ("Johanni27", "album1J"))
    print(s.recv(500))

    # print("CREATE ALBUM \t%s\t%s" % ("Sarah58", "album1S"))
    # send(s, "CREATE ALBUM \t%s\t%s\r\n" % ("Sarah58", "album1S"))
    # print(s.recv(500))
    #
    # print("CREATE ALBUM \t%s\t%s" % ("Preslava11", "album1P"))
    # send(s, "CREATE ALBUM \t%s\t%s\r\n" % ("Preslava11", "album1P"))
    # print(s.recv(500))

    # print("CREATE ALBUM %s" % "album2")
    # send(s, "CREATE ALBUM %s\r\n" % "album2")
    # print(s.recv(500))
    #
    # print("CREATE PHOTO \talbum1\tI love \\t Grapes!!!\tgrapes.png\t450 \t 320")
    # send(s, "CREATE PHOTO \talbum1\tI love \\t Grapes!!!\tgrapes.png\t450 \t 320\r\n")
    # print(s.recv(500))

    # print("DEL PHOTO \talbum2\tI love \\t Grapes!!!")
    # send(s, "DEL PHOTO \talbum2\tI love \\t Grapes!!!\r\n")
    # print(s.recv(500))
    #
    # print("SET ALBUM \talbum2\ttitle\tnew_album")
    # send(s, "SET ALBUM \talbum2\ttitle\tnew_album\r\n")
    # print(s.recv(500))
    #
    # print("SET ALBUM \talbum1\ttitle\talbum100")
    # send(s, "SET ALBUM \talbum1\ttitle\talbum100\r\n")
    # print(s.recv(500))
    #
    # print("SET PHOTO \talbum1\tI love \\t Grapes!!!\ttitle\tnew_photo\tfilename\tnew_grapes.png")
    # send(s, "SET PHOTO \talbum1\tI love \\t Grapes!!!\ttitle\tnew_photo\tfilename\tnew_grapes.png\r\n")
    # print(s.recv(500))

    # print("SET PHOTO \talbum1\tI love \\t Grapes!!!\ttitle\ttomatoes\tfilename\tgrapes.png")
    # send(s, "SET PHOTO \talbum1\tI love \\t Grapes!!!\ttitle\ttomatoes\tfilename\tnew_grapes.png\r\n")
    # print(s.recv(500))

    # print("GET PHOTO \talbum1\tphoto1")
    # send(s, "GET PHOTO \talbum1\tphoto1\r\n")
    # print(s.recv(500))
    #
    # print("GET ALBUM \talbum1")
    # send(s, "GET ALBUM \talbum1\r\n")
    # print(s.recv(500))
    #
    # print("GET ALBUM USER\tJohanni100\talbum1")
    # send(s, "GET ALBUM USER\tJohanni100\talbum1\r\n")
    # print(s.recv(500))

    #set photo
    #get functions
    #how encode actual images
    #
    # print("DEL ALBUM \t%s" % "album2")
    # send(s, "DEL ALBUM \t%s\r\n" % "album")
    # print(s.recv(500))

    #f = open("images_client1/graph.png", "rb")
    #l = f.read()
    #send(s, )
    #send(s, file)
    #send(s, "\r\n")
    print("QUIT")
    send(s, "QUIT\r\n")
    print(s.recv(500))


# connect to load balancer and get a hostname and portnum for server
print("trying to connect to Load Balancer")
(hostname_s, portnum_s) = setup_connection(host, port)
print("succesffully connected with Load Balancer")
print("Connection with hostname %s and port %s" % (hostname_s, portnum_s))
sendmsg(1, hostname_s, portnum_s, fromaddr, toaddr)
