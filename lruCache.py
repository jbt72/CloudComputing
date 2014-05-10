__author__ = 'johanni27'

# Cache implementaion with a Least Recently Used (LRU) replacement policy and
# a basic dictionary interface.


class dlnode(object):
    def __init__(self):
        self.empty = True

class lruCache(object):

    def __init__(self, size, callback=None):
        self.callback = callback
        self.htble = {}

        # Initialize the doubly linked list with one empty node. This is an
        # invariant. The cache size must always be greater than zero. Each
        # node has a 'prev' and 'next' variable to hold the node that comes
        # before it and after it respectivly. Initially the two variables
        # each point to the head node itself, creating a circular doubly
        # linked list of size one. Then the size() method is used to adjust
        # the list to the desired size.

        self.head = dlnode()
        self.head.next = self.head
        self.head.prev = self.head
        self.listSize = 1


    def len(self):
        return len(self.htble)

    def clear(self):
        node = self.head
        for i in range(len(self.table)):
            node.empty = True
            node.key = None
            node.value = None
            node = node.next

        self.htble.clear()

    def contains(self, key):
        return key in self.htble

    # Looks up a value in the cache without affecting cache order
    def peek(self, key):
        node = self.htble[key]
        return node.value

    # This method adjusts the ordering of the doubly linked list so that
    # 'node' directly precedes the 'head' node. Because of the order of
    # operations, if 'node' already directly precedes the 'head' node or if
    # 'node' is the 'head' node the order of the list will be unchanged.
    def reOrder(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev

        node.prev = self.head.prev
        node.next = self.head.prev.next

        node.next.prev = node
        node.prev.next = node

    def get(self, key, default=None):
        try:
            node = self.htble[key]

            # Update the list ordering. Move this node so that it directly
            # proceeds the head node. Then set the 'head' variable to it. This
            # makes it the new head of the list.
            self.reOrder(node)
            self.head = node

            return node.value
        except KeyError:
            return default

    def set(self, key, value):
        # First, see if any value is stored under 'key' in the cache already.
        # If so we are going to replace that value with the new one.
        if key in self.htble:
            node = self.htble[key]
            node.value = value
            self.reOrder(node)
            self.head = node
            return

        # Ok, no value is currently stored under 'key' in the cache. We need
        # to choose a node to place the new item in. There are two cases. If
        # the cache is full some item will have to be pushed out of the
        # cache. We want to choose the node with the least recently used
        # item. This is the node at the tail of the list. If the cache is not
        # full we want to choose a node that is empty. Because of the way the
        # list is managed, the empty nodes are always together at the tail
        # end of the list. Thus, in either case, by chooseing the node at the
        # tail of the list our conditions are satisfied.

        # Since the list is circular, the tail node directly preceeds the
        # 'head' node.
        node = self.head.prev

        # If the node already contains something we need to remove the old
        # key from the dictionary.
        if not node.empty:
            if self.callback is not None:
                self.callback(node.key, node.value)
            del self.htble[node.key]

        # Place the new key and value in the node
        node.empty = False
        node.key = key
        node.value = value

        #Add the node to the dictionary under the new key
        self.htble[key] = node

        # We need to move the node to the head of the list. The node is the
        # tail node, so it directly preceeds the head node due to the list
        # being circular. Therefore, the ordering is already correct, we just
        # need to adjust the 'head' variable.
        self.head = node

    def delitem(self, key):

        # Lookup the node, then remove it from the hash table.
        node = self.htble[key]
        del self.htble[key]

        node.empty = True

        # Not strictly necessary.
        node.key = None
        node.value = None

        # Because this node is now empty we want to reuse it before any
        # non-empty node. To do that we want to move it to the tail of the
        # list. We move it so that it directly preceeds the 'head' node. This
        # makes it the tail node. The 'head' is then adjusted. This
        # adjustment ensures correctness even for the case where the 'node'
        # is the 'head' node.
        self.reOrder(node)
        self.head = node.next


    def size(self, size=None):
        if size is not None:
            assert size > 0
            if size > self.listSize:
                self.addTailNode(size - self.listSize)
            elif size < self.listSize:
                self.removeTailNode(self.listSize - size)

        return self.listSize

    # Increases the size of the cache by inserting n empty nodes at the tail
    # of the list.
    def addTailNode(self, n):
        for i in range(n):
            node = dlnode()
            node.next = self.head
            node.prev = self.head.prev

            self.head.prev.next = node
            self.head.prev = node

        self.listSize += n

    # Decreases the size of the list by removing n nodes from the tail of the
    # list.
    def removeTailNode(self, n):
        assert self.listSize > n
        for i in range(n):
            node = self.head.prev
            if not node.empty:
                if self.callback is not None:
                    self.callback(node.key, node.value)
                del self.table[node.key]

            # Splice the tail node out of the list
            self.head.prev = node.prev
            node.prev.next = self.head

            node.prev = None
            node.next = None

            node.key = None
            node.value = None

        self.listSize -= n


c = lruCache(100)
print("len %s" % c.len())
key = "lkjadlkasjfsadjfasldfjasldfj_lskjdfalsdjfadfa_lkjflkjsdfasd"
value = "{kdljfalsdjf,;jdfa;lskdjfasmaskdfjoefkjfsdf{{{}}}}}"
print("cache contains %s" % c.contains(key))
c.set(key, value)
print("key was added to cache")
print("cache contains %s" % c.contains(key))
print("value %s" % c.get(key))
c.set(key, value + "blahhhhhhhhhhh")
print("value was updated")
print("value %s" % c.get(key))
c.delitem(key)
print("item was deleted")
print("cache contains %s" % c.contains(key))