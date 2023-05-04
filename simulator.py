import numpy as np
from abc import ABC, abstractmethod
from generator import workload_generator


class block_server:
    """
    a simulated block server
    """
    def __init__(self, block_size):
        self.block_size = block_size
        self.pending_tnxs = list()
        self.processed_op_count = 0
        self.success_op_count = 0
    
    def accept_new_tnxs(self, new_tnxs):
        self.pending_tnxs += new_tnxs

    def process_block(self):
        """
        Process the first block_size transactions
        by counting the write-write and read-read conflicts
        """
        actual_block_size = min(self.block_size, len(self.pending_tnxs))
        if actual_block_size == 0:
            return None, None
        block_tnxs = self.pending_tnxs[0:actual_block_size]
        self.pending_tnxs = self.pending_tnxs[actual_block_size:]
        rset, wset = set(), set()
        for tnx in block_tnxs:
            if tnx.tnx_type == 'read':
                rset.add(tnx.key)
            else:
                wset.add(tnx.key)
        invalid_keys = rset.intersection(wset)
        tnx_valid = [tnx.key not in invalid_keys for tnx in block_tnxs]
        self.processed_op_count += actual_block_size
        self.success_op_count += np.sum(tnx_valid)
        return block_tnxs, tnx_valid


class transaction:
    def __init__(self, user, tnx_type, key):
        self.user, self.tnx_type, self.key = user, tnx_type, key


class peer(ABC):
    """
    Abstract class of peer simulator
    """

    @abstractmethod
    def next_tnx(self):
        pass


class simple_rw_peer(peer):
    """
    Peer with simple read-and-write access pattern
        - first, read an arbitary key
        - then, write the same key
    """

    def __init__(self, name: str, generator: workload_generator):
        self.waiting = False
        self.name = name
        self.workload_generator = generator
        self.tnx_history = list()
    

    def next_tnx(self):
        """
        Generate the next transaction submitted by this peer
        - if waiting for previous response, do not submit a new transaction
        - else, if previous tnx is read, submit a write request with the same key
            otherwise, submit a read request with a new key
        """
        if self.waiting:
            return None
        if len(self.tnx_history) > 0 and self.tnx_history[-1].tnx_type == 'read':
            new_tnx = transaction(self.name, 'write', self.tnx_history[-1].key)
        else:
            new_tnx = transaction(self.name, 'read', self.workload_generator.generate(1)[0])
        self.waiting = True
        self.tnx_history.append(new_tnx)
        return new_tnx


    def flip_state(self):
        """
        Change the state of current waiting status
        """
        self.waiting = not self.waiting
