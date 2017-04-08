import os

#=== system / project

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

PORT_BASE = 2330

TCP_BUFFER_SIZE = 4096

#=== experiment config

# M/M/1 queue arrival rate
QUEUE_LAMB = 30
# M/M/1 queue service rate
QUEUE_MU = 200

# inter-node communication delay, T * size + P
TRANSMIT_DELAY = 0.00
PROPAGATE_DELAY = 0.00
## processing delay for each tuple
# COMPUTING_DELAY = 0.000001
# GEN_DELAY = 0.00002

# the space cost is approximated by moving average: old = old * (1 - F) + new * F
FADING_FACTOR = 0.2

# number interval between two consecutive barriers
BARRIER_INTERVAL = 25
# the interval between two consecutive time measurement
TIME_AUDIT_INTERVAL = 30
# the interval between two consecutive space measurement
SPACE_AUDIT_INTERVAL = 30