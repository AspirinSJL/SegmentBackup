import CONSTANTS
from tuple import *

import logging
import os
import xmlrpclib
import thread
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
        if self.type == 'filter':
            self.operator = lambda t: filter(self.rule, [t])
        elif self.type == 'transform':
            self.operator = lambda t: map(self.rule, [t])
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

        # TODO: lock?
        self.input_queues = dict()
        for n in upstream_nodes:
            self.input_queues[n] = {'is_blocked': False, 'queue': deque()}

        # self.rpc_server = SimpleXMLRPCServer(('localhost', self.port))

    def handle_tuple(self, node_id, tuple_):
        if isinstance(tuple_, BarrierTuple):
            self.handle_barrier(node_id, tuple_)
        else:
            output = self.rule(tuple_)
            for t in output:
                for n in self.downstream_nodes:
                    try:
                        sock = socket.create_connection(('localhost', CONSTANTS.PORT_BASE + n))
                        sock.sendall(output)
                    except socket.error, e:
                        LOGGER.error(e)
                    finally:
                        sock.close()

        self.cur_node_state = tuple_.tuple_id
        
    def handle_barrier(self, node_id, barrier):
        # backup for node
        # if this is the last barrier needed for a version, checkpoint a version
        if all(self.input_queues[n]['is_blocked'] for n in self.upstream_nodes if n != node_id):
            # checkpoint
            open(os.path.join(self.node_backup_dir, barrier.version)).close()
            self.latest_checked_version = barrier.version

            for n in self.upstream_nodes:
                if n != node_id:
                    self.input_queues[n]['is_blocked'] = False
        else:
            self.input_queues[node_id]['is_blocked'] = True

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = json.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))
            self.handle_tuple(data)


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

    def manage_version_ack(self, node_id, version):
        self.pending_window.manage_version_ack(node_id, version)

    def handle_tuple(self, node_id, tuple_):
        super(ConnectingNode, self).handle_tuple(node_id, tuple_)


    def handle_barrier(self, node_id, barrier):
        # backup for node
        super(ConnectingNode, self).handle_barrier(node_id, barrier)

        # ack version for pending window
        self.ack_version(barrier.version)

    def serve_inbound_connection(self):
        while True:
            conn, client = self.sock.accept()
            data = json.loads(conn.recv(CONSTANTS.TCP_BUFFER_SIZE))

            if isinstance(data, Tuple):
                self.handle_tuple(data)
            else:
                # built-in tuple type
                self.manage_version_ack(data)
