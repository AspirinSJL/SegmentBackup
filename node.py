import CONSTANTS
from tuple import BarrierTuple

import os
import xmlrpclib
import thread
from collections import deque
from SimpleXMLRPCServer import SimpleXMLRPCServer


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

    def rewind(self, ):
        pass


class Node(object):
    """docstring for Node"""
    def __init__(self, node_id, operator, upstream_nodes, downstream_nodes):
        self.node_id = node_id
        self.operator = operator
        self.upstream_nodes = upstream_nodes
        self.downstream_nodes = downstream_nodes
        
        self.upstream_node_proxies = dict()
        for n in self.upstream_nodes:
            self.upstream_node_proxies[n] = xmlrpclib.ServerProxy('localhost', CONSTANTS.PORT_BASE + n)

        self.downstream_node_proxies = dict()
        for n in self.downstream_nodes:
            self.downstream_node_proxies[n] = xmlrpclib.ServerProxy('localhost', CONSTANTS.PORT_BASE + n)

        self.cur_node_state = 0
        
        self.latest_checked_version = None
        
        self.port = CONSTANTS.PORT_BASE + self.node_id

        self.backup_dir = os.path.join(CONSTANTS.ROOT_DIR, 'backup', node_id)

        self.node_backup_dir = os.path.join(self.backup_dir, 'node')

        self.input_queues = dict()
        for n in upstream_nodes:
            self.input_queues[n] = {'is_blocked': False, 'queue': deque()}

        self.rpc_server = SimpleXMLRPCServer(('localhost', self.port))

    def resolve_tuple(self, node_id, tuple_):
        if isinstance(tuple_, BarrierTuple):
            self.resolve_barrier(node_id, tuple_)
        else:
            output = self.operator(tuple_)
            for t in output:
                for n in self.downstream_node_proxies:
                    n.resolve_tuple(self.node_id, t)

        self.cur_node_state = tuple_.tuple_id
        
    def resolve_barrier(self, node_id, barrier):
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

    def run(self):
        self.rpc_server.register_instance(self)
        thread.start_new_thread(self.rpc_server.serve_forever, ())

        while True:
            pass


class ConnectingNode(Node):
    """docstring for ConnectingNode"""
    def __init__(self, node_id, operator, upstream_nodes, downstream_nodes,
                 upstream_cut=None, downstream_cut=None):
        super(ConnectingNode, self).__init__(node_id, operator, upstream_nodes, downstream_nodes)

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

    def resolve_tuple(self, node_id, tuple_):
        super(ConnectingNode, self).resolve_tuple(node_id, tuple_)



    def resolve_barrier(self, node_id, barrier):
        # backup for node
        super(ConnectingNode, self).resolve_barrier(node_id, barrier)

        # ack version for pending window
        self.ack_version(barrier.version)


