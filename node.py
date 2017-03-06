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
    def __init__(self, downstream_cut, backup_dir):
        # each pending window (or node) only has a single downstream cut,
        # otherwise inconsistency occurs during truncating
        self.downstream_cut = downstream_cut
        self.backup_dir = backup_dir

        # each backup file is named by the ending version, so the current writing one is named temporarily
        self.current_file = open(os.path.join(self.backup_dir, 'current'), 'w')

        self.version_acks = dict()
        for n in self.downstream_cut:
            self.version_acks[n] = deque()

    def append(self, tuple_):
        """Make an output tuple persistent, and complete a version if necessary
        """

        self.current_file.write(tuple_.tuple_id + '\n')

        if isinstance(tuple_, BarrierTuple):
            self.current_file.close()
            os.rename(os.path.join(self.backup_dir, 'current'),
                os.path.join(self.backup_dir, tuple_.version))

            self.current_file = open(os.path.join(self.backup_dir, 'current'), 'w')

    def extend(self, tuples):
        # TODO: can be improved
        for t in tuple:
            self.append(t)

    def truncate(self, version):
        """Delete files with filename <= version
        """

        for f in os.listdir(self.backup_dir):
            if f.isdigit() and int(f) <= version:
                os.remove(os.path.join(self.backup_dir, f))

    def handle_version_ack(self, version_ack):
        self.version_acks[version_ack.sent_from].append(version_ack.version)

        if all(self.version_acks.values()) and set(map(lambda q: q[0], self.version_acks.values())) == 1:
            self.truncate(version_ack.version)

            for q in self.version_acks.values():
                q.popleft()

    def rewind(self, version):
        """Delete files with filename > version
        """

        for f in os.listdir(self.backup_dir):
            if f == 'current' or int(f) > version:
                os.remove(os.path.join(self.backup_dir, f))

        self.current_file = open(os.path.join(self.backup_dir, 'current'), 'w')


class Node(object):
    """Normal Node as if SegmentBackup is not considered.
    """
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

        # update after handling each tuple
        self.cur_tuple_handling_state = 0

        # update after handling each app tuple
        self.cur_operator_computing_state = 0

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

    def multicast(self, group, msg):
        for n in group:
            try:
                sock = socket.create_connection(('localhost', CONSTANTS.PORT_BASE + n))
                sock.sendall(json.dumps(msg))
            except socket.error, e:
                LOGGER.error(e)
            finally:
                sock.close()

    def handle_tuple(self, tuple_):
        """ General method to handle any received tuple, including sending out if applicable
        """
        if isinstance(tuple_, BarrierTuple):
            self.handle_barrier(tuple_)
        else:
            self.handle_normal_tuple(tuple_)

        self.cur_tuple_handling_state = tuple_.tuple_id

    def handle_normal_tuple(self, tuple_):
        output = self.operator(tuple_)
        self.cur_operator_computing_state = tuple_.tuple_id

        self.multicast(self.downstream_nodes, output)

        return output

    def handle_barrier(self, barrier):
        """ Return whether a version is completed
        """

        # if this is the last barrier needed for a version, checkpoint a version and relay the barrier to downstream
        if all(self.input_queues[n].is_blocked for n in self.upstream_nodes if n != barrier.sent_from):
            # checkpoint (touch a file)
            open(os.path.join(self.node_backup_dir, barrier.version)).close()
            self.latest_checked_version = barrier.version

            # can relay the barrier now
            self.multicast(self.downstream_nodes, [barrier])

            # open all the channels after each checkpoint
            for n in self.upstream_nodes:
                if n != barrier.sent_from:
                    self.input_queues[n].is_blocked = False

            return True
        else:
            # stop handling the tuples from this sender to wait for others
            self.input_queues[barrier.sent_from].is_blocked = True

            return False

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

        self.pending_window_backup_dir = os.path.join(self.backup_dir, 'pending_window')

        self.pending_window = PendingWindow(self, self.pending_window_backup_dir)

    def ack_version(self, version):
        # TODO: multiple upstream cuts should be valid too
        self.multicast(self.upstream_cut, VersionAck(self.node_id, version))

    # TODO: base method use base method or ?
    def handle_normal_tuple(self, tuple_):
        output = super(ConnectingNode, self).handle_normal_tuple(tuple_)

        self.pending_window.extend(output)

    def handle_barrier(self, barrier):
        is_version = super(ConnectingNode, self).handle_barrier(barrier)

        if is_version:
            self.pending_window.append(barrier)
            self.ack_version(barrier.version)

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = json.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, VersionAck):
                # TODO: add buffer and threading
                self.pending_window.handle_version_ack(data)
            elif isinstance(data, list):
                assert data and isinstance(data[0], Tuple)
                for t in data:
                    self.input_queues[t.sent_from].queue.put(t, block=True)
            else:
                LOGGER.warn('received unknown data type %s' % type(data))