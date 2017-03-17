import time
import CONSTANTS

class TestTimer(object):
    def __init__(self, node):
        self.node = node

        self.start = time.time()

        self.start_new = None

        self.pending_window_write = 0
        self.pending_window_handle_ack = 0

        self.handle_barrier = 0
        self.handle_normal = 0

    def read(self):
        self.node.LOGGER.info('''%s
        total:            %f
        delay before new: %f
        pwnd write:       %f
        pwnd handle ack:  %f
        handle barrier:   %f
        handle normal:    %f
        ''' % ('-' * 10 + str(self.node.node_id),
               time.time() - self.start, self.start_new - self.start,
               self.pending_window_write, self.pending_window_handle_ack,
               self.handle_barrier, self.handle_normal))

    def run(self):
        while True:
            time.sleep(CONSTANTS.TIMER_INTERVAL)
            self.read()