__author__ = 'johanni27'

import getopt
import socket
import sys
from Queue import Queue
from threading import Thread, Timer, Lock, Condition
import time
import re
from collections import deque
import datetime
import pymongo
from pymongo import MongoClient



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
# TODO: when creating check for uniqueness
# TODO: when updating, setting, deleting, check object exists. If not send error message using socket
class Commands:
    def __init__(self, socket):
        self.socket = socket
        self.options = {'CREATE ALBUM': self.create_album_handle,
                        'CREATE PHOTO': self.create_photo_handle,
                        'DEL ALBUM': self.del_album_handle,
                        'DEL PHOTO': self.del_photo_handle,
                        'SET ALBUM': self.set_album_handle,
                        'SET PHOTO': self.set_photo_handle,
                        'GET ALBUM': self.get_album_handle,
                        'GET PHOTO': self.get_photo_handle,
                        'PING': self.ping_handle,
                        'QUIT': self.quit_handle}


    def command_handle(self, command):
        c_keyword = " ".join(command.split(" ")[0:2])
        print("command %s" %  c_keyword)
        if (c_keyword in self.options):
            print("before call")
            return self.options[c_keyword](command)
        else:
            print("Unrecognized commands")
            return -1

    def create_album_handle(self, command):
        name = command.split(" ")[2]
        album = {"title": name, "creation_date": datetime.datetime.utcnow(),
                 "modify_date": datetime.datetime.utcnow(), "images": []}
        db.albums.save(album)
        db.users.update( {"name": username},
                         {"$addToSet": {"albums": album["_id"]} } )
        db.users.update( {"name": username}, {"$set": {"modify_date": datetime.datetime.utcnow()}})
        self.socket.send("+OK\r\n")
        return 0

    def create_photo_handle(self, command):
        attr = command.split("\t")
        # TODO: have an actual image here
        photo = {"title": attr[2], "creation_date": datetime.datetime.utcnow(),
                 "modify_date": datetime.datetime.utcnow(), "filename": attr[3],
                 "width": attr[4], "height": attr[5]}
        db.photos.save(photo)

        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            if (db.albums.find_one({"_id": album_id})["title"] == attr[1]):
                db.albums.update( {"_id": album_id}, {"$addToSet": {"images": photo["_id"]}})
                db.albums.update( {"_id": album_id}, {"$set": {"modify_date": datetime.datetime.utcnow()}})

        self.socket.send("+OK\r\n")
        return 0


    def del_album_handle(self, command):
        album_name = command.split("\t")[1]
        user = db.users.find_one({"name": username})

        for album_id in user["albums"]: # iterate through the users albums
            album = db.albums.find_one({"_id": album_id})
            # check if album has right title
            if album["title"] == album_name:
                for photo_id in album["images"]:
                    db.photos.remove({"_id": photo_id})
                 # We remove all children images of this album since no image is referenced
                 # by the multiple album
                 # POTIONIAL OPTIMIZATION: allow images to be referenced multiple times
                db.albums.remove( {"_id": album_id} )
                db.users.update({"name": username}, {"$pull": {"albums": album_id}})

        self.socket.send("+OK\r\n")
        return 0



    def del_photo_handle(self, command):
        photo_name = command.split("\t")[2]
        album_name = command.split("\t")[1]

        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            album = db.albums.find_one({"_id": album_id})
            if (album["title"] == album_name):
                for photo_id in album["images"]:
                    if (db.photos.find_one({"_id": photo_id})["title"] == photo_name):
                        db.photos.remove({"_id": photo_id})
                        db.albums.update({"_id": album_id}, {"$pull": {"images": photo_id}} )

        self.socket.send("+OK\r\n")
        return 0

    # This does not update the images feature
    def set_album_handle(self, command):
        attr = command.split("\t")
        album_name = attr[1]
        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            album = db.albums.find_one({"_id": album_id})

            if (album["title"] == album_name):
                i = 2
                while i < len(attr):
                    db.albums.update({"_id": album["_id"]}, {"$set": {attr[i]: attr[i+1]} })
                    i+= 2

                db.albums.update( {"title": album_name},
                                  {"$set": {"modify_date": datetime.datetime.utcnow()}} )
        self.socket.send("+OK\r\n")
        return 0


    def set_photo_handle(self, command):
        attr = command.split("\t")
        album_name = attr[1]
        photo_name = attr[2]
        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            album = db.albums.find_one({"_id": album_id})
            if (album["title"] == album_name):
                for photo_id in album["images"]:
                    if (db.photos.find_one({"_id": photo_id})["title"] == photo_name):
                        i = 3
                        while i < len(attr):
                            db.photos.update({"_id": photo_id}, {"$set": {attr[i]: attr[i+1]} })
                            i+= 2

        self.socket.send("+OK\r\n")
        return 0

    # Socket simply sends the string version of the JSON object
    def get_photo_handle(self, command):
        attr = command.split("\t")
        album_name = attr[1]
        photo_name = attr[2]
        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            album = db.albums.find_one({"_id": album_id})
            if (album["title"] == album_name):
                for photo_id in album["images"]:
                    photo = db.photos.find_one({"_id": photo_id})
                    if (photo["title"] == photo_name):
                        self.socket.send(str(photo))
        return 0

    def get_album_handle(self, command):
        attr = command.split("\t")
        album_name = attr[1]
        user = db.users.find_one({"name": username})
        for album_id in user["albums"]: # iterate through the users albums
            # check if album has right title
            album = db.albums.find_one({"_id": album_id})
            if (album["title"] == album_name):
                self.socket.send(str(album))
        return 0

    def quit_handle(self, command):
        self.socket.send("+OK\r\n")
        return 1


    # PING the server
    def ping_handle(self, command):
        self.socket.send("PONG\r\n")
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
        self.slaves_sockets = []
        # for (hostname, portnum) in slaves:
        #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     s.connect((hostname, portnum))
        #     self.slaves_sockets.append(s)


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
        self.command = self.unprocessed_packets.partition(self.partitioner)[0] #\r\n is removed
        print("new command %s" % self.command)
        self.unprocessed_packets = self.unprocessed_packets.partition(self.partitioner)[2]
        print("new unprocessed packets %s" % self.unprocessed_packets)


    # checks for valid client
    def authentication_handle(self):
        global username
        while (not self.valid_client):
            # check if valid_client appears on list
            self.collect_input()
            username = self.command
            print("trying username %s" % username)
            if (db.users.find_one({"name": self.command})):
                print("found username")
                self.collect_input()
                print("after collection with username")
                if (db.users.find_one({"name": username, "password": self.command})):
                    print("was valid")
                    self.valid_client = True
                    self.timeout.cancel()
                    self.reset_timer()
                    self.socket.send("+OK\r\n")
                    print("sent info")
                else:
                    self.socket.send("-Error 1.2 Invalid password\r\n")
            else:
                self.socket.send("-Error 1.1 Username does not exist\r\n")


    def handle(self):
        try:
            print("sent")
            self.socket.send("South Korea Server")
            print("after sent")
            self.timeout.start()
            self.authentication_handle()
            print("finished authentication")
            c = Commands(self.socket)
            print("before loop")
            while (not self.complete):
                print("gathering input")
                self.collect_input()
                print("received input")
                request = c.command_handle(self.command)
                if (request == 0):
                    self.reset_timer()
                if (request == 1): # quiting
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
    print("serverloop in server host %s and port %s" % (host, port))
    serversocket.bind((host, port))
    # start listening with a backlog of 5 connections
    # backlog is # of pending connections to allow
    serversocket.listen(5)
    print("success")

    while True:
        # accept a connection
        # s.accept() blocks until connection received
        # address = [host, port]
        # TODO: block if more than 32
        with num_conn_lock:
            if (num_connections < 1):
                (clientsocket, address) = serversocket.accept()
                ct = ConnectionHandler(clientsocket)
                thread_pool.add_job(ct)
                num_connections += 1

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
netID = "jbt72"
database = {"mykey": "Hello"}
slaves = {("127.0.0.2", 8766)}


num_conn_lock = Lock()
num_connections = 0
host = "00000"
port = 0000
username = ""


def connect_db():
    global db
    client = MongoClient('mongodb://Johanni27:1234@ds047207.mongolab.com:47207/motherland')
    db = client.motherland


class Server:
    def __init__(self, hostname, port_num):
        global host
        host = hostname
        global port
        port = port_num
        print("Server coming up on %s:%i" % (host, port))
        # Connect to DB
        connect_db()
        serverloop()
