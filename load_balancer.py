import getopt
import socket
import sys
import random
import server
from Queue import Queue
from threading import Thread, Timer, Lock, Condition
import time
import re
from collections import deque
import os


host = "127.0.0.1"
port = 8790
type = 1 #0: random 1: round-robbin 2: least_connections 3: least connections round-robbin
counter = 0


# ===================================
# WORKER
# ===================================

class Worker(Thread):
    def __init__(self, ll, lock, condition):
        Thread.__init__(self)
        self.daemon = True
        self.jobs = ll
        self.ll_lock = lock
        self.occupied = condition

    def run(self):
        global num_jobs
        global num_conn_lock
        global num_connections
        while True:
            # grab work from queue syncrohnized
            with self.ll_lock:
                while not (num_jobs > 0):
                    self.occupied.wait()
                num_jobs -= 1
                self.work = self.jobs.popleft()

            # Do work
            self.work.handle()
            with num_conn_lock:
                num_connections -= 1


# ===================================
# ThreadPool
# ===================================

class ThreadPool:
    # spawn a pool of daemon threads & pass them an instance of the queue
    def __init__(self, num_threads):
        self.jobs = deque([])
        self.lock = Lock()
        self.occupied = Condition(self.lock)
        for i in range(num_threads):
            t = Worker(self.jobs, self.lock, self.occupied)
            t.start()

    # add a job to the queue, in this case connections
    def add_job(self, socket):
        global num_jobs
        with self.lock:
            self.jobs.append(socket)
            num_jobs += 1
            self.occupied.notify()


# ===================================
# Commands
# ===================================

class Commands:
    def __init__(self, socket):
        self.socket = socket
        self.options = {'CONNECT': self.connect_handle}
        self.pick = 0


    def command_handle(self, command):
        print(command)
        c_keyword = command.split(" ")[0]
        print(c_keyword)
        if (c_keyword in self.options):
            return self.options[c_keyword](command)
        else:
            print("Unrecognized commands")
            return -1

    # TODO Develop algorithm for which server client should connect with
    # TODO Mixture of server workload and distance from client
    # TODO Not about master of user, since on flikr people tend to view other
    # TODO people's albums more than one's one
    # TODO creae a list and history for client
    def connect_handle(self, command):
        global counter
        if (type == 0): #random
            self.pick = random.randint(0, len(servers) - 1)
        if (type == 1): #round-robbin
            if (counter >= len(servers)):
                counter = 0
            self.pick = counter
            counter += 1
        self.socket.send(servers[self.pick][0] +
                         " " + str(servers[self.pick][1]) + " \r\n")
        return 0



# ===================================
# Connection Handler
# ===================================

# handle a single client request
# Each client is handled by a separate thread
class ConnectionHandler:
    def __init__(self, socket):
        self.socket = socket
        self.complete = False
        self.clienthostname = ""
        self.timeout = Timer(3.0, self.handle_timeout)
        self.valid_client = False
        self.unprocessed_packets = ""
        self.command = ""

    def handle_timeout(self):
        if (self.complete == False):
            self.socket.send("South Korea Server Error: timeout exceeded")
        self.socket.close()
        self.complete = True

    def reset_timer(self):
        self.timeout = Timer(3.0, self.handle_timeout)
        self.timeout.start()

    def collect_input(self):
        self.partitioner = "\r\n"
        print ("Before loop: unprocessed packets %s" % self.unprocessed_packets)
        while (self.unprocessed_packets.partition(self.partitioner)[1] == ""):
            self.unprocessed_packets += self.socket.recv(500)
            #print ("In loop: unprocessed packets %s" % self.unprocessed_packets)
        self.command = self.unprocessed_packets.partition(self.partitioner)[0]
        print("new command %s" % self.command)
        self.unprocessed_packets = self.unprocessed_packets.partition(self.partitioner)[2]
        print("new unprocessed packets %s" % self.unprocessed_packets)


    def handle(self):
        try:
            self.socket.send("Asia Load Balancer")
            self.timeout.start()
            c = Commands(self.socket)
            while (not self.complete):
                self.collect_input()
                print(self.command)
                request = c.command_handle(self.command)
                if (request == 0):
                    self.reset_timer()
                    self.complete = True
        except:
            self.handle_timeout
            return


# ===================================
# Main Server Loop
# ===================================

# the main server loop
def serverloop():
    # initialize ThreadPool
    thread_pool = ThreadPool(max_workers)

    global num_conn_lock
    global num_connections

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # mark the socket so we can rebind quickly to this port number
    # after the socket is closed
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # bind the socket to the local loopback IP address and special port
    serversocket.bind((host, port))
    # start listening with a backlog of 5 connections
    # backlog is # of pending connections to allow
    serversocket.listen(5)

    while True:
        # accept a connection
        # s.accept() blocks until connection received
        # address = [host, port]
        # TODO: block if more than 32
        with num_conn_lock:
            if (num_connections < 5):
                (clientsocket, address) = serversocket.accept()
                ct = ConnectionHandler(clientsocket)
                thread_pool.add_job(ct)
                num_connections += 1

def child_server(info):
    (hostname, portnum, db_conn_address) = info
    s = server.Server(hostname, int(portnum), db_conn_address, server_addresses)


def start_servers():
    for server in servers:
        newpid = os.fork()
        if newpid == 0:
            child_server(server)




# ===================================
# main
# ===================================

# You don't have to change below this line.  You can pass command-line arguments
# -h/--host [IP] -p/--port [PORT] to put your server on a different IP/port.
opts, args = getopt.getopt(sys.argv[1:], 'h:p:', ['host=', 'port='])

for k, v in opts:
    if k in ('-h', '--host'):
        host = v
    if k in ('-p', '--port'):
        port = int(v)

# initialize global variables
max_workers = 32
num_jobs = 0

# [ (ip_address, port_num, db_conn_address) ]
servers = [ ("127.0.0.1", "8769", "mongodb://Johanni27:1234@ds047207.mongolab.com:47207/motherland"),
            ("127.0.0.1", "8770", "mongodb://Johanni271:1234@ds033257.mongolab.com:33257/tomorrowland"),
            ("127.0.0.1", "8778", "mongodb://Johanni272:1234@ds043037.mongolab.com:43037/candyland")]
server_addresses = {"motherland": ("127.0.0.1", "8769"), "tomorrowland": ("127.0.0.1", "8770"),
                    "candyland": ("127.0.0.1", "8778")}


num_conn_lock = Lock()
num_connections = 0

print("Gateway Server coming up on %s:%i" % (host, port))

# Create & start the servers
start_servers()


serverloop()
__author__ = 'johanni27'

