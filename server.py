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
from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
import lruCache




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
            # grab work from queue synchronized
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
    def __init__(self, socket, db_name, db, usersdb, server_send_sockets):
        self.socket = socket
        self.db_name = db_name
        self.db = db
        self.usersdb = usersdb
        self.server_send_sockets = server_send_sockets
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
        attr = command.split("\t")
        album = {"title": attr[2], "creation_date": datetime.datetime.utcnow(),
                 "modify_date": datetime.datetime.utcnow(), "images": []}
        print("WAS AT RIGHT PLACE")
        album_master = self.usersdb.users.find_one({"name": attr[1]})["master"]
        print("1")
        if album_master == self.db_name:
            print("2")
            self.db.albums.save(album)
            print("3")
            self.db.users.update( {"name": attr[1]},
                             {"$addToSet": {"albums": album["_id"]} } )
            print("4")
            self.db.users.update( {"name": attr[1]},
                                  {"$set": {"modify_date": datetime.datetime.utcnow()}})
            print("5")
            self.socket.send("+OK\r\n")
            print("6")
        else:
            print("about to ask another server %s" % album_master)
            print("current db is %s" % self.db_name)
            print(self.server_send_sockets)
            print("in create album it did it")
            album_m_s = self.server_send_sockets["candyland"]
            print("fetched socket for other server")
            album_m_s.send(command + "\r\n")
            print("successfully sent command")
            response = album_m_s.recv(500)
            print("received resonspe")
            if (response == "+OK\r\n"):
                self.socket.send("+OK\r\n")
            else:
                self.socket.send("Failure to ask another server")


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
                # key: user_id\r\album_name\r\nphoto_name
                cache.setitem(user["_id"] + "\r\n" + attr[1] + "\r\n" + attr[2], str(photo))
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
                db.albums.remove({"_id": album_id})
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

    # TODO check if server is master of object
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


    # def get_album_handle(self, command):
    #     attr = command.split("\t")
    #     album_name = attr[1]
    #     user = db.users.find_one({"name": username})
    #     for album_id in user["albums"]: # iterate through the users albums
    #         # check if album has right title
    #         album = db.albums.find_one({"_id": album_id})
    #         if album["title"] == album_name:
    #             self.socket.send(str(album))
    #     return 0

    # specifies a user
    def get_album_handle(self, command):
        attr = command.split("\t")
        if (len(attr) < 3):
            album_username = username
        album_username = attr[1]
        album_name = attr[2]
        # find master server
        album_master_name = users_db.users.find_one({"name": album_username})["master"]
        if (album_master_name == master):
            user = db.users.find_one({"name": album_username})
            for album_id in user["albums"]: # iterate through the users albums
                # check if album has right title
                album = db.albums.find_one({"_id": album_id})
                if album["title"] == album_name:
                    self.socket.send(str(album))
        else:
            # TODO check normal cache
            # if not there than ask info
            socket = master_sockets[album_master_name]
            send(socket, "GET ALBUM \talbum_name\r\n")
            self.socket.send(s.recv(500))


        return 0

    def quit_handle(self, command):
        self.socket.send("+OK\r\n")
        return 1


    # PING the server
    def ping_handle(self, command):
        self.socket.send("PONG\r\n")
        return 0





# ===========================================
# Initialize Global Variables and Functions
# ===========================================
max_workers = 32
num_jobs = 0
num_connections = 0
num_conn_lock = Lock()

def send(socket, message):
    # In Python 3, must convert message to bytes explicitly.
    # In Python 2, this does not affect the message.
    socket.send(message.encode('utf-8'))


# =========================
# Server
# =========================

class Server:
    def __init__(self, hostname, port_num, db_conn_address, server_addresses):
        self.host = hostname
        self.port = port_num
        self.server_addresses = server_addresses
        self.num_server_send_conn = 0
        self.num_server_recv_conn = 0
        self.db_name = "asdf"
        self.db = "lkj"
        # Server A -> Server B
        # commandSenderB -> commandReceiverA
        # commandSenderB <- commandReceiverA
        # where A,B is any unique combo of Servers motherland, tomorrowland, and candyland
        # the receive sockets have their own threads in ServerConnectionHandler
        self.server_send_sockets = {}

        print("Server coming up on %s:%i" % (self.host, self.port))

        # Set up connection with assigned Database
        self.connect_db(db_conn_address)

        # Set up connection with Database in charge of user->master dictionary
        client = MongoClient("mongodb://Johanni273:1234@ds029847.mongolab.com:29847/userland")
        self.usersdb = client.userland

        # Set up communication between servers
        #connect_to_servers(server_ss)

        self.serverloop()

    def connect_db(self, db_address):
        client = MongoClient(db_address)
        arr = db_address.split("/")
        self.db_name = arr[len(arr) - 1]
        if (self.db_name == "motherland"):
            self.db = client.motherland
        elif (self.db_name == "tomorrowland"):
            self.db = client.tomorrowland
        elif (self.db_name == "candyland"):
            self.db = client.candyland
        else:
            print("Error: Unable to connect to a database")


    # ===================================
    # Main Server Loop
    # ===================================

    def serverloop(self):
        # initialize ThreadPool
        thread_pool = ThreadPool(max_workers)

        global num_conn_lock
        global num_connections

        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # mark the socket so we can rebind quickly to this port number
        # after the socket is closed
        serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # bind the socket to the local loopback IP address and special port
        print("serverloop in server host %s and port %s" % (self.host, self.port))
        serversocket.bind((self.host, self.port))
        # start listening with a backlog of 5 connections
        # backlog is # of pending connections to allow
        serversocket.listen(5)


        # =======================================================================
        # Initialize Sockets in charge of Server CommandSender Communication
        # =======================================================================
        print("dbname is %s" % self.db_name)
        if (self.db_name == "motherland"):
            # send requests to tomorrowland and candy land
            while self.num_server_send_conn < 1:
                try: # Server motherland requests connection from Server candyland
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.server_addresses["candyland"][0],
                                     int(self.server_addresses["candyland"][1])))
                    s.recv(500)
                    s.send(self.db_name)
                    self.server_send_sockets["candyland"] = s
                    album_m_s = self.server_send_sockets["candyland"]
                    print("successfully accesed dicti!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    self.num_server_send_conn += 1
                except socket.error:
                    a = 2
                    #print("CONNECTION REFUSED Motherland")
            while self.num_server_send_conn < 2:
                try: # Server motherland requests connection from Server tomorrowland
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.server_addresses["tomorrowland"][0],
                                     int(self.server_addresses["tomorrowland"][1])))
                    s.recv(500)
                    s.send(self.db_name)
                    self.server_send_sockets["tomorrowland"] = s
                    self.num_server_send_conn += 1
                except socket.error:
                    a = 2
                    #print("CONNECTION REFUSED Motherland")
        elif (self.db_name == "tomorrowland"):
            # Server tomorrowland requests connection from Server candyland
            print("tomorrowland creating socket")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("tomorrowland created socket")
            s.connect((self.server_addresses["candyland"][0],
                             int(self.server_addresses["candyland"][1])))
            print("tomorrowland connected")
            s.recv(500)
            print("tomorrowland received")
            s.send(self.db_name)
            print("tomorrowland sent data")
            self.server_send_sockets["candyland"] = s
            self.num_server_send_conn += 1
            # Server tomorrowland accepts request by Server motherland
            while self.num_server_send_conn < 2:
                with num_conn_lock:
                    (otherserversocket, address) = serversocket.accept()
                    otherserversocket.send(self.db_name)
                    othersocketname = otherserversocket.recv(500)
                    self.server_send_sockets[othersocketname] = otherserversocket
                    self.num_server_send_conn += 1
        elif (self.db_name == "candyland"):
            # Server candyland accepts request by Server motherland and tomorrowland
            while self.num_server_send_conn < 2:
                with num_conn_lock:
                    print("trying to accept")
                    (otherserversocket, address) = serversocket.accept()
                    print("accepted")
                    otherserversocket.send(self.db_name)
                    print("candyland sent data")
                    othersocketname = otherserversocket.recv(500)
                    print("candyland received data %s" % othersocketname)
                    self.server_send_sockets[othersocketname] = otherserversocket
                    self.num_server_send_conn += 1
                    print("inc conn")
        else:
            print("Error not a master")
        print("master RECV %s got out!!" % self.db_name)

        # =======================================================================
        # Initialize Sockets in charge of Server CommandReceiver Communication
        # =======================================================================

        if (self.db_name == "motherland"):
            # send requests to tomorrowland and candy land
            while self.num_server_recv_conn < 1:
                try: # Server motherland requests connection from Server candyland
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.server_addresses["candyland"][0],
                                     int(self.server_addresses["candyland"][1])))
                    t = ServerConnectionHandler(s, True, self.db_name, self.db,
                                                self.usersdb, self.server_send_sockets)
                    t.start()
                    self.num_server_recv_conn += 1
                except socket.error:
                    a =2
                    #print("CONNECTION REFUSED Motherland")
            while self.num_server_recv_conn < 2:
                try: # Server motherland requests connection from Server tomorrowland
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.server_addresses["tomorrowland"][0],
                                     int(self.server_addresses["tomorrowland"][1])))
                    t = ServerConnectionHandler(s, True, self.db_name, self.db,
                                                self.usersdb, self.server_send_sockets)
                    t.start()
                    self.num_server_recv_conn += 1
                except socket.error:
                    a=2
                    #print("CONNECTION REFUSED Motherland")
        elif (self.db_name == "tomorrowland"):
            # Server tomorrowland requests connection from Server candyland
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.server_addresses["candyland"][0],
                             int(self.server_addresses["candyland"][1])))
            t = ServerConnectionHandler(s, True, self.db_name, self.db,
                                        self.usersdb, self.server_send_sockets)
            t.start()
            self.num_server_recv_conn += 1
            # Server tomorrowland accepts request by Server motherland
            while self.num_server_recv_conn < 2:
                with num_conn_lock:
                    (otherserversocket, address) = serversocket.accept()
                    t = ServerConnectionHandler(otherserversocket, False, self.db_name,
                                                self.db, self.usersdb, self.server_send_sockets)
                    t.start()
                    self.num_server_recv_conn += 1
        elif (self.db_name == "candyland"):
            # Server candyland accepts request by Server motherland and tomorrowland
            while self.num_server_recv_conn < 2:
                with num_conn_lock:
                    (otherserversocket, address) = serversocket.accept()
                    t = ServerConnectionHandler(otherserversocket, False, self.db_name,
                                                self.db, self.usersdb, self.server_send_sockets)
                    t.start()
                    self.num_server_recv_conn += 1
        else:
            print("Error not a master")
        print("master %s got out!!" % self.db_name)

        # ======================================================
        # Initialize Sockets in charge of Client Communication
        # ======================================================

        while True:
            with num_conn_lock:
                if (num_connections < 5):
                    (clientsocket, address) = serversocket.accept()
                    ct = ClientConnectionHandler(clientsocket, self.server_send_sockets,
                                                 self.db_name, self.db, self.usersdb)
                    thread_pool.add_job(ct)
                    num_connections += 1
                    print("num connections (worker threads) %s" % num_connections)


# ===================================
# Server Connection Handler
# ===================================

# Spawn a thread to handle all requests by servers
class ServerConnectionHandler(Thread):
    def __init__(self, socket, initiator, db_name, db, usersdb, server_send_sockets):
        Thread.__init__(self)
        self.daemon = True
        socket.settimeout(None)
        self.socket = socket
        self.unprocessed_packets = ""
        self.command = ""
        self.otherSocketName = ""
        self.db_name = db_name
        self.db = db
        self.usersdb = usersdb
        self.server_send_sockets = server_send_sockets # only need this for command
        print("tyring to print server connection")
        print(self.server_send_sockets)
        print("finsihed tryign to print")
        self.server_send_sockets = server_send_sockets
        if initiator:
            print("I am the initator and am trying to figure it out")
            self.otherSocketName = self.socket.recv(500)
            self.socket.send(self.db_name)
        else:
            print("trying to send and am not the initator")
            self.socket.send(self.db_name)
            self.otherSocketName = self.socket.recv(500)

    def collect_input(self):
        self.partitioner = "\r\n"
        print ("Before loop: unprocessed packets %s" % self.unprocessed_packets)
        while (self.unprocessed_packets.partition(self.partitioner)[1] == ""):
            try:
                print("serverconnectionhandler trying to receive")
                print("socket %s that runs commands in db %s for server %s" % (self.socket, self.db_name,
                                                                            self.otherSocketName))
                print(self.socket())
                self.unprocessed_packets += self.socket.recv(500)
                print("recv is done bitches. Suckers")
            except socket.error:
                print("!!!!WAH SERVER CONNECTION SOCKET DIED TRYING TO RECEIVE")
            print("!!!!!!serverconnectionhandler unproc pack %s" % self.unprocessed_packets)
            #print ("In loop: unprocessed packets %s" % self.unprocessed_packets)
        self.command = self.unprocessed_packets.partition(self.partitioner)[0] #\r\n is removed
        print("new command %s" % self.command)
        self.unprocessed_packets = self.unprocessed_packets.partition(self.partitioner)[2]
        print("new unprocessed packets %s" % self.unprocessed_packets)

    def run(self):
        c = Commands(self.socket, self.db_name, self.db, self.usersdb, self.server_send_sockets)
        while True:
            print("serverconnectionhandler is inside while loop")
            self.collect_input()
            print("INPUT WAS COLLECTED")
            c.command_handle(self.command)


# ===================================
# Connection Handler
# ===================================

# handle a single client request
# Each client is handled by a separate thread
class ClientConnectionHandler:
    def __init__(self, socket, server_send_sockets, db_name, db, usersdb):
        self.socket = socket
        self.complete = False
        self.timeout = Timer(50.0, self.handle2_timeout)
        self.valid_client = False
        self.unprocessed_packets = ""
        self.command = ""
        self.server_send_sockets = server_send_sockets
        self.db_name = db_name
        self.db = db
        self.usersdb = usersdb
        self.username = "jjj"
        print("trying to print")
        print(self.server_send_sockets)
        print("finished printing")
        print(self.server_send_sockets["candyland"])
        print("3successfully accesed dicti!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

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


    def handle2_timeout(self):
        print("in handle222 timeout")
        if (self.complete == False):
            self.socket.send("South Korea Server Error: timeout exceeded")
        self.socket.close()
        self.complete = True

    def handle_timeout(self):
        print("in handle timeout")
        if (self.complete == False):
            self.socket.send("South Korea Server Error: timeout exceeded")
        self.socket.close()
        self.complete = True

    def reset_timer(self):
        print("ABOUT TO TIMEOUT")
        self.timeout = Timer(20.0, self.handle_timeout)
        #self.timeout.start()

    # checks for valid client
    def authentication_handle(self):
        global username
        while (not self.valid_client):
            # check if valid_client appears on list
            self.collect_input()
            self.username = self.command
            print("trying username %s" % self.username)
            print(self.db_name)
            print(self.db)
            print('dfj')
            if (self.db.users.find_one({"name": self.command})):
                print("found username")
                self.collect_input()
                print("after collection with username")
                if (self.db.users.find_one({"name": self.username, "password": self.command})):
                    print("was valid")
                    self.valid_client = True
                    self.timeout.cancel()
                    print("reset timer in authentication")
                    self.reset_timer()
                    self.socket.send("+OK\r\n")
                    print("sent info")
                else:
                    self.socket.send("-Error 1.2 Invalid password\r\n")
            else:
                self.socket.send("-Error 1.1 Username does not exist\r\n")


    def handle(self):
        #try:
            print("sent")
            self.socket.send("South Korea Server")
            print("after sent")
            self.timeout.start()
            self.authentication_handle()
            print("finished authentication")
            c = Commands(self.socket, self.db_name, self.db, self.usersdb,
                         self.server_send_sockets)
            print("before loop")
            while (not self.complete):
                print("gathering input")
                self.collect_input()
                print("received input")
                request = c.command_handle(self.command)
                if (request == 0):
                    print("request %s" % request)
                    print("reset timer in handle request of client connection")
                    self.reset_timer()
                if (request == 1): # quiting
                    self.complete = True
        #except:
            #self.handle_timeout
            return

