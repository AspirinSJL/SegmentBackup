import os

#=== system / project

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

PORT_BASE = 2330

TCP_BUFFER_SIZE = 4096

#=== experiment config

# inter-node communication delay, T * size + P
TRANSMIT_DELAY = 0.001
PROPAGATE_DELAY = 1
# processing delay for each tuple
COMPUTING_DELAY = 0.4
GEN_DELAY = 0.2

# the space cost is approximated by moving average: old = old * (1 - F) + new * F
FADING_FACTOR = 0.2

# number interval between two consecutive barriers
BARRIER_INTERVAL = 25
# the interval between two consecutive time measurement
TIME_AUDIT_INTERVAL = 30
# the interval between two consecutive space measurement
SPACE_AUDIT_INTERVAL = 30