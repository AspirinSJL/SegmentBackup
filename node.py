import CONSTANTS
from tuple import *

import logging
import os
import xmlrpclib
import thread
import Queue
import socket
import json
from collections import deque


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Separate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)

LOGGER = logging.getLogger('Node')


class PendingWindow(object):
    """docstring for PendingWindow"""
    def __init__(self, node, backup_dir):
        self.node = node
        self.directory = directory
        self.current_file = open(os.path.join(self.directory, 'current'), 'w')

        self.version_acks = dict()
        for n in self.node.downstream_cut:
            self.version_acks[n] = []

    def append(self, tuple_):
        self.current_file.write(tuple_.tuple_id + '\n')

        if isinstance(tuple_, BarrierTuple):
            self.current_file.close()
            os.rename(os.path.join(self.directory, 'current'),
                os.path.join(self.directory, tuple_.version))

            self.current_file = open(os.path.join(self.directory, 'current'), 'w')

    def truncate(self, version):
        """Delete files with filename <= version
        """

        for f in os.listdir(self.directory):
            if int(f) <= version:
                os.remove(os.path.join(self.directory, f))

    def manage_version_ack(self, node_id, version):
        self.version_acks[node_id].append(version)

        if all(self.version_acks.values()) and set(map(lambda q: q[-1], self.version_acks.values())) == 1:
            self.truncate(version)

    def rewind(self, version):
        for f in os.listdir(self.directory):
            if f == 'current' or int(f) > version:
                os.remove(os.path.join(self.directory, f))

        self.current_file = open(os.path.join(self.directory, 'current'), 'w')


class Node(object):
    """Normal Node as if SegmentBackup is not considered"""
    def __init__(self, node_id, type, rule, upstream_nodes, downstream_nodes):
        self.node_id = node_id
        self.type = type
        self.rule = rule
        self.upstream_nodes = upstream_nodes
        self.downstream_nodes = downstream_nodes
        
        # construct an operator according to node type and rule
        # the operator usually takes in a Tuple and return a list of Tuples
        # TODO: make operator more flexible
        if self.type == 'filter':
            self.operator = lambda t: filter(self.rule, [t])
        elif self.type == 'transform':
            self.operator = lambda t: map(self.rule, [t])
        elif self.type == 'reduce':
            pass
        elif self.type == 'join':
            pass
        elif self.type in ('source', 'sink'):
            pass
        else:
            LOGGER.error('%s is not implemented' % self.type)

        # socket for Tuple passing
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('localhost', CONSTANTS.PORT_BASE + self.node_id))
        self.sock.listen(2)

        # update after handling each Tuple
        self.cur_node_state = 0

        # update after handling each BarrierTuple
        self.latest_checked_version = None

        # create backup directories
        self.backup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'backup', node_id)
        self.node_backup_dir = os.path.join(self.backup_dir, 'node')
        for d in (self.backup_dir, self.node_backup_dir):
            os.makedirs(d)

        # if a queue is blocked, it only buffer the incoming tuples but not handle them
        # i.e., blocked refers to the HEAD of the queue
        class InputQueue(object):
            def __init__(self):
                self.is_blocked = False
                self.queue = Queue.Queue()

        self.input_queues = dict()
        for n in upstream_nodes:
            self.input_queues[n] = InputQueue()

    def handle_tuple(self, tuple_):
        ''' General method to handle any received tuple, including sending out if applicable
        '''

        def handle_normal_tuple(tuple_):
            output = self.operator(tuple_)
            for n in self.downstream_nodes:
                try:
                    sock = socket.create_connection(('localhost', CONSTANTS.PORT_BASE + n))
                    sock.sendall(json.dumps(output))
                except socket.error, e:
                    LOGGER.error(e)
                finally:
                    sock.close()

        def handle_barrier(barrier):
            # if this is the last barrier needed for a version, checkpoint a version
            if all(self.input_queues[n].is_blocked for n in self.upstream_nodes if n != barrier.sent_from):
                # checkpoint (touch a file)
                open(os.path.join(self.node_backup_dir, barrier.tuple_id)).close()

                self.latest_checked_version = barrier.tuple_id

                # open all the channels after each checkpoint
                for n in self.upstream_nodes:
                    if n != barrier.sent_from:
                        self.input_queues[n].is_blocked = False
            else:
                # stop handling the tuples from this sender to wait for others
                self.input_queues[barrier.sent_from].is_blocked = True

        if isinstance(tuple_, BarrierTuple):
            handle_barrier(tuple_)
        else:
            handle_normal_tuple(tuple_)

        self.cur_node_state = tuple_.tuple_id

    def consume_buffered_tuples(self):
        # round robin
        while True:
            for q in self.input_queues.values():
                if q.queue.empty() or q.is_blocked:
                    continue

                self.handle_tuple(q.queue.get())
                q.queue.task_done()

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = json.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))
            assert data and isinstance(data, list) and isinstance(data[0], Tuple)
            # put the tuple list into according buffer for later handling
            for t in data:
                self.input_queues[t.sent_from].queue.put(t, block=True)


class ConnectingNode(Node):
    """docstring for ConnectingNode"""
    def __init__(self, node_id, type, rule, upstream_nodes, downstream_nodes,
                 upstream_cut=None, downstream_cut=None):
        super(ConnectingNode, self).__init__(node_id, type, rule, upstream_nodes, downstream_nodes)
        
        self.upstream_cut = upstream_cut
        self.downstream_cut = downstream_cut

        self.upstream_cut_proxies = dict()
        for n in self.upstream_cut:
            self.upstream_cut_proxies[n] = xmlrpclib.ServerProxy('localhost', CONSTANTS.PORT_BASE + n)

        self.downstream_cut_proxies = dict()
        for n in self.downstream_cut:
            self.downstream_cut_proxies[n] = xmlrpclib.ServerProxy('localhost', CONSTANTS.PORT_BASE + n)

        self.pending_window_backup_dir = os.path.join(self.backup_dir, 'pending_window')

        self.pending_window = PendingWindow(self, self.pending_window_backup_dir)

    def ack_version(self, version):
        for n in self.upstream_cut:
            # RPC
            n.manage_version_ack(self.node_id, version)

    def handle_tuple(self, tuple_):
        super(ConnectingNode, self).handle_tuple(tuple_)

        if isinstance(tuple_, BarrierTuple):
            self.pending_window.manage_version_ack()

            # TODO: state change should be here?

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = json.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, Tuple):
                self.handle_tuple(data)
            else:
                # built-in tuple type
                self.manage_version_ack(data)
