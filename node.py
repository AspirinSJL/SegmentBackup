#!/usr/bin/env python

import CONSTANTS
from tuple import *
from pending_window import *
from utility.auditor import *

import logging
import os
import time
import thread
import Queue
import socket
import pickle
import hdfs
from sys import getsizeof


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(threadName)-10s %(message)s',
    # Separate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)


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

        self.hdfs_client = hdfs.Config().get_client('dev')

        # create backup directories
        # TODO: parent dir should be an argument
        self.backup_dir = os.path.join('backup', str(node_id))
        self.node_backup_dir = os.path.join(self.backup_dir, 'node')
        for d in (self.backup_dir, self.node_backup_dir):
            self.hdfs_client.makedirs(d)

    def prepare(self):
        """Operate before each running
            and some info should be reset before each run
            because some things can't be pickled, of course
        """

        logging.getLogger('hdfs.client').setLevel(logging.WARNING)

        # for measuring the delay before processing new tuples
        self.last_run_state = max(map(int, self.hdfs_client.list(self.node_backup_dir)) or [0])

        self.time_auditor = TimeAuditor(self)
        self.space_auditor = SpaceAuditor(self)
        thread.start_new_thread(self.time_auditor.run, ())
        thread.start_new_thread(self.space_auditor.run, ())

        self.LOGGER = logging.getLogger('Node %d' % self.node_id)

        # socket for passing tuple
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('localhost', CONSTANTS.PORT_BASE + self.node_id))
        self.sock.listen(2)

    def multicast(self, group, msg, is_audit_other=False):
        if msg in (None, []):
            return

        # assume multicast infra
        time.sleep(CONSTANTS.TRANSMIT_DELAY * getsizeof(msg) + CONSTANTS.PROPAGATE_DELAY)
        
        if is_audit_other:
            self.space_auditor.network_other += getsizeof(msg)
        else:
            self.space_auditor.network_normal += getsizeof(msg)
        
        # modify sent_from field
        if isinstance(msg, VersionAck):
            msg.sent_from = self.node_id
        else:
            for t in msg:
                t.sent_from = self.node_id

        # send to each destination in group
        for n in group:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # self.LOGGER.debug('connecting to node %d from node %d' % (n, self.node_id))
                sock.connect(('localhost', CONSTANTS.PORT_BASE + n))
                # TODO: non-blocking?
                sock.sendall(pickle.dumps(msg))
                # self.LOGGER.debug('sent to node %d from node %d' % (n, self.node_id))
            except socket.error, e:
                self.LOGGER.error(e)
            finally:
                sock.close()

    def checkpoint_version(self, version):
        # touch a file
        # TODO: change overwrite to rewind
        self.hdfs_client.write(os.path.join(self.node_backup_dir, str(version)), data='', overwrite=True)
        self.latest_checked_version = version


class Spout(Node):
    """Source node with connecting node features
    """

    def __init__(self, node_id, type, downstream_nodes, downstream_connectors, computing_state=0):
        super(Spout, self).__init__(node_id, type, computing_state)

        self.downstream_nodes = downstream_nodes
        self.downstream_connectors = downstream_connectors

        self.pending_window_backup_dir = os.path.join(self.backup_dir, 'pending_window')
        self.pending_window = PendingWindow(self.pending_window_backup_dir, self)

    def gen_tuple(self):
        self.time_auditor.start_new = time.time()

        # step from the last computing state
        i = self.computing_state + 1
        while True:
            if i % CONSTANTS.BARRIER_INTERVAL == 0:
                output = [BarrierTuple(i, self.node_id, i)]
                self.checkpoint_version(i)
            else:
                output = [Tuple(i, self.node_id)]
                self.computing_state = i

            tick = time.time()
            self.pending_window.extend(output)
            tock = time.time()
            self.time_auditor.pending_window_write += tock - tick

            self.multicast(self.downstream_nodes, output, is_audit_other=(i % CONSTANTS.BARRIER_INTERVAL == 0))
            self.tuple_handling_state = i

            time.sleep(CONSTANTS.GEN_DELAY)
            i += 1

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, VersionAck):
                # TODO: add buffer and threading
                tick = time.time()
                self.pending_window.handle_version_ack(data)
                tock = time.time()
                self.time_auditor.pending_window_handle_ack += tock - tick
            else:
                self.LOGGER.warn('received unknown data type %s' % type(data))

    def run(self, replay=False):
        self.prepare()

        if replay:
            self.pending_window.replay()

        thread.start_new_thread(self.serve_inbound_connection, ())
        thread.start_new_thread(self.gen_tuple, ())

        while True:
            pass


class Bolt(Node):
    """Normal operating node without SegmentBackup
    """

    def __init__(self, node_id, type, rule, upstream_nodes, downstream_nodes=None, computing_state=0):
        super(Bolt, self).__init__(node_id, type, computing_state)

        self.rule = rule
        self.upstream_nodes = upstream_nodes
        self.downstream_nodes = downstream_nodes

    def prepare(self):
        super(Bolt, self).prepare()

        # if a queue is blocked, it only buffer the incoming tuples but not handle them
        # i.e., blocked refers to the HEAD of the queue
        self.input_queues = dict()
        for n in self.upstream_nodes:
            self.input_queues[n] = {
                'is_blocked': False,
                'queue': Queue.Queue()
            }

        # construct an operator according to node type and rule
        # the operator usually takes in a Tuple and return a list of Tuples
        # TODO: make operator more flexible
        # TODO: should in __init__
        if self.type == 'filter':
            self.operator = lambda t: filter(eval(self.rule), [t])
        elif self.type == 'transform':
            self.operator = lambda t: map(eval(self.rule), [t])
        elif self.type == 'reduce':
            pass
        elif self.type == 'join':
            pass
        elif self.type == 'sink':
            pass
        else:
            self.LOGGER.error('%s is not implemented' % self.type)

    def handle_tuple(self, tuple_):
        """General method to handle any received tuple, including sending out if applicable
        """
        if self.time_auditor.start_new == None and tuple_.tuple_id > self.last_run_state:
            self.time_auditor.start_new = time.time()

        time.sleep(CONSTANTS.COMPUTING_DELAY)

        if isinstance(tuple_, BarrierTuple):
            tick = time.time()
            self.handle_barrier(tuple_)
            tock = time.time()
            self.time_auditor.handle_barrier += tock - tick
        else:
            tick = time.time()
            self.handle_normal_tuple(tuple_)
            tock = time.time()
            self.time_auditor.handle_normal += tock - tick

        self.tuple_handling_state = tuple_.tuple_id

    def handle_normal_tuple(self, tuple_):
        if self.type == 'sink' and self.rule == 'print and store':
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
        if all(self.input_queues[n]['is_blocked'] for n in self.upstream_nodes if n != barrier.sent_from):
            self.checkpoint_version(barrier.version)

            # can relay the barrier now
            if self.type != 'sink':
                self.multicast(self.downstream_nodes, [barrier], is_audit_other=True)

            # open all the channels after each checkpoint
            for n in self.upstream_nodes:
                if n != barrier.sent_from:
                    self.input_queues[n]['is_blocked'] = False

            return True
        else:
            # stop handling the tuples from this sender to wait for others
            self.input_queues[barrier.sent_from]['is_blocked'] = True

            return False

    def consume_buffered_tuples(self):
        # round robin
        while True:
            for q in self.input_queues.values():
                if q['queue'].empty() or q['is_blocked']:
                    continue

                self.handle_tuple(q['queue'].get())
                q['queue'].task_done()

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            assert data and isinstance(data, list) and isinstance(data[0], Tuple)
            # put the tuple list into the according buffer for later handling
            for t in data:
                self.input_queues[t.sent_from]['queue'].put(t, block=True)

    def run(self):
        self.prepare()

        thread.start_new_thread(self.serve_inbound_connection, ())
        thread.start_new_thread(self.consume_buffered_tuples, ())

        while True:
            pass


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
        self.multicast(self.upstream_connectors, VersionAck(self.node_id, version), is_audit_other=True)

    def handle_normal_tuple(self, tuple_):
        output = super(Connector, self).handle_normal_tuple(tuple_)

        if self.type != 'sink' or self.rule == 'print and store':
            tick = time.time()
            self.pending_window.extend(output)
            tock = time.time()
            self.time_auditor.pending_window_write += tock - tick

    def handle_barrier(self, barrier):
        is_version = super(Connector, self).handle_barrier(barrier)

        if is_version:
            tick = time.time()
            self.pending_window.append(barrier)
            tock = time.time()
            self.time_auditor.pending_window_write += tock - tick

            self.ack_version(barrier.version)

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = pickle.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            # self.LOGGER.debug('received data')

            if isinstance(data, VersionAck):
                # TODO: add buffer and threading
                tick = time.time()
                self.pending_window.handle_version_ack(data)
                tock = time.time()
                self.time_auditor.pending_window_handle_ack += tock - tick
            elif isinstance(data, list):
                assert data and isinstance(data[0], Tuple)
                for t in data:
                    self.input_queues[t.sent_from]['queue'].put(t, block=True)
            else:
                self.LOGGER.warn('received unknown data type %s' % type(data))

    def run(self, replay=False):
        self.prepare()

        if replay:
            self.pending_window.replay()

        thread.start_new_thread(self.serve_inbound_connection, ())
        thread.start_new_thread(self.consume_buffered_tuples, ())

        while True:
            pass