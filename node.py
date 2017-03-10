#!/usr/bin/env python

import CONSTANTS
from tuple import *

import logging
import os
import time
import thread
import Queue
import socket
import pickle
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
    def __init__(self, backup_dir, node):
        # TODO: not cut
        # each pending window (or node) only has a single downstream cut,
        # otherwise inconsistency occurs during truncating

        self.backup_dir = backup_dir
        os.mkdir(self.backup_dir)

        self.node = node

        # each backup file is named by the ending version, so the current writing one is named temporarily
        self.current_file = open(os.path.join(self.backup_dir, 'current'), 'wb')

        # the version that last truncation conducted against
        self.safe_version_file = open(os.path.join(self.backup_dir, 'safe_version'), 'w')
        self.safe_version_file.write(str(0))

        if self.node.type != 'sink':
            self.version_acks = dict()
            for n in self.node.downstream_connectors:
                self.version_acks[n] = deque()

    def append(self, tuple_):
        """Make an output tuple persistent, and complete a version if necessary
        """

        pickle.dump(tuple_, self.current_file)

        if isinstance(tuple_, BarrierTuple):
            self.current_file.close()
            os.rename(os.path.join(self.backup_dir, 'current'),
                os.path.join(self.backup_dir, str(tuple_.version)))

            self.current_file = open(os.path.join(self.backup_dir, 'current'), 'w')

    def extend(self, tuples):
        # TODO: can be improved
        for t in tuples:
            self.append(t)

    def truncate(self, version):
        """Delete files with filename <= version
        """

        self.safe_version_file.seek(0)
        self.safe_version_file.write(str(version))
        # note that this 'truncate()' means differently in Python from our definition
        self.safe_version_file.truncate()

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

    def replay(self):
        """When both the node and pending window state are ready, replay the pending window before resuming
        """

        for v in sorted(os.listdir(self.backup_dir)):
            tuples = []
            with open(os.path.join(self.backup_dir, v), 'rb') as f:
                # TODO: incomplete writing
                while True:
                    try:
                        tuples.append(pickle.load(f))
                    except EOFError:
                        break

            self.node.multicast(self.node.downstream_nodes, tuples)


class Node(object):
    """Basic node with basic utility
    """

    def __init__(self, node_id, type, computing_state=0):
        self.node_id = node_id
        self.type = type
        # update after handling (creating if Spout) each app tuple
        self.computing_state = computing_state

        # update after handling each tuple
        self.tuple_handling_state = None

        # update after handling (creating if Spout) each BarrierTuple
        self.latest_checked_version = None

        # create backup directories
        # TODO: parent dir should be an argument
        self.backup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'backup', str(node_id))
        self.node_backup_dir = os.path.join(self.backup_dir, 'node')
        for d in (self.backup_dir, self.node_backup_dir):
            os.makedirs(d)

    def multicast(self, group, msg):
        for n in group:
            try:
                sock = socket.create_connection(('localhost', CONSTANTS.PORT_BASE + n))
                # TODO: non-blocking?
                sock.sendall(pickle.dumps(msg))
            except socket.error, e:
                LOGGER.error(e)
            finally:
                sock.close()

    def checkpoint_version(self, version):
        # touch a file
        open(os.path.join(self.node_backup_dir, version)).close()
        self.latest_checked_version = version

    def run(self):
        # socket for passing tuple
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('localhost', CONSTANTS.PORT_BASE + self.node_id))
        self.sock.listen(2)

class Spout(Node):
    """Source node with connecting node features
    """

    def __init__(self, node_id, type, downstream_nodes, downstream_connectors, delay, barrier_interval, computing_state=0):
        super(Spout, self).__init__(node_id, type, computing_state)

        self.downstream_nodes = downstream_nodes
        self.downstream_connectors = downstream_connectors
        self.delay = delay
        self.barrier_interval = barrier_interval

        self.pending_window_backup_dir = os.path.join(self.backup_dir, 'pending_window')
        self.pending_window = PendingWindow(self.pending_window_backup_dir, self)

    def gen_tuple(self):
        # step from the last computing state
        i = self.computing_state + 1
        while True:
            if i % self.barrier_interval:
                output = [BarrierTuple(i, self.node_id, i)]
                self.checkpoint_version(i)
            else:
                output = [Tuple(i, self.node_id)]
                self.computing_state = i

            self.multicast(self.downstream_nodes, output)
            self.tuple_handling_state = i

            time.sleep(self.delay)
            i += 1

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, VersionAck):
                # TODO: add buffer and threading
                self.pending_window.handle_version_ack(data)
            else:
                LOGGER.warn('received unknown data type %s' % type(data))

    def run(self):
        super(Spout, self).run()

        thread.start_new_thread(self.serve_inbound_connection, ())
        thread.start_new_thread(self.gen_tuple, ())

# if a queue is blocked, it only buffer the incoming tuples but not handle them
# i.e., blocked refers to the HEAD of the queue
class InputQueue(object):
    def __init__(self):
        self.is_blocked = False
        self.queue = Queue.Queue()


class Bolt(Node):
    """Normal operating node without SegmentBackup
    """

    def __init__(self, node_id, type, rule, upstream_nodes, downstream_nodes=None, computing_state=0):
        super(Bolt, self).__init__(node_id, type, computing_state)

        self.rule = rule
        self.upstream_nodes = upstream_nodes
        self.downstream_nodes = downstream_nodes

    def prepare(self):
        self.input_queues = dict()
        for n in self.upstream_nodes:
            self.input_queues[n] = InputQueue()

        # construct an operator according to node type and rule
        # the operator usually takes in a Tuple and return a list of Tuples
        # TODO: make operator more flexible
        # TODO: should in __init__
        if self.type == 'filter':
            self.operator = lambda t: filter(self.rule, [t])
        elif self.type == 'transform':
            self.operator = lambda t: map(self.rule, [t])
        elif self.type == 'reduce':
            pass
        elif self.type == 'join':
            pass
        else:
            LOGGER.error('%s is not implemented' % self.type)

    def handle_tuple(self, tuple_):
        """General method to handle any received tuple, including sending out if applicable
        """
        if isinstance(tuple_, BarrierTuple):
            self.handle_barrier(tuple_)
        else:
            self.handle_normal_tuple(tuple_)

        self.tuple_handling_state = tuple_.tuple_id

    def handle_normal_tuple(self, tuple_):
        if self.type == 'sink' and self.rule == 'print and store':
            print tuple_.tuple_id
            self.computing_state = tuple_.tuple_id
            return [tuple_]
        else:
            output = self.operator(tuple_)
            self.computing_state = tuple_.tuple_id

            self.multicast(self.downstream_nodes, output)
            return output

    def handle_barrier(self, barrier):
        """Return whether a version is completed
        """

        # if this is the last barrier needed for a version, checkpoint a version and relay the barrier to downstream
        if all(self.input_queues[n].is_blocked for n in self.upstream_nodes if n != barrier.sent_from):
            self.checkpoint_version(barrier.version)

            # can relay the barrier now
            if self.type != 'sink':
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
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            assert data and isinstance(data, list) and isinstance(data[0], Tuple)
            # put the tuple list into according buffer for later handling
            for t in data:
                self.input_queues[t.sent_from].queue.put(t, block=True)

    def run(self):
        self.prepare()

        thread.start_new_thread(self.serve_inbound_connection, ())
        thread.start_new_thread(self.consume_buffered_tuples, ())


class Connector(Bolt):
    """docstring for ConnectingNode"""

    def __init__(self, node_id, type, rule, upstream_nodes, upstream_connectors,
                 downstream_nodes=None, downstream_connectors=None, computing_state=0):
        super(Connector, self).__init__(node_id, type, rule, upstream_nodes, downstream_nodes, computing_state)
        
        self.upstream_connectors = upstream_connectors
        self.downstream_connectors = downstream_connectors

        self.pending_window_backup_dir = os.path.join(self.backup_dir, 'pending_window')

        self.pending_window = PendingWindow(self.pending_window_backup_dir, self)

    def ack_version(self, version):
        # TODO: multiple upstream cuts should be valid too
        self.multicast(self.upstream_connectors, VersionAck(self.node_id, version))

    def handle_normal_tuple(self, tuple_):
        output = super(Connector, self).handle_normal_tuple(tuple_)

        if self.type != 'sink' or self.rule == 'print and store':
            self.pending_window.extend(output)

    def handle_barrier(self, barrier):
        is_version = super(Connector, self).handle_barrier(barrier)

        if is_version:
            self.pending_window.append(barrier)
            self.ack_version(barrier.version)

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, VersionAck):
                # TODO: add buffer and threading
                self.pending_window.handle_version_ack(data)
            elif isinstance(data, list):
                assert data and isinstance(data[0], Tuple)
                for t in data:
                    self.input_queues[t.sent_from].queue.put(t, block=True)
            else:
                LOGGER.warn('received unknown data type %s' % type(data))