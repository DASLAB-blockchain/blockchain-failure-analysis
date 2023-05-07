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
        rset, wset, tnx_valid = set(), set(), list()
        for tnx in block_tnxs:
            if tnx.tnx_type == 'read':
                tnx_valid.append(tnx.key not in wset)
                rset.add(tnx.key)
            else:
                tnx_valid.append((tnx.key not in rset) and (tnx.key not in wset))
                wset.add(tnx.key)
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
        self.last_failed = False
        self.name = name
        self.workload_generator = generator
        self.tnx_history = list()
    

    def check_if_next_transaction(self):
        """
        Return if the machine has next transaction available to submit
        """
        if self.last_failed:
            return True
        return not self.waiting
    

    def update_peer_state(self, server_response):
        """
        Update the fail/waiting status of the peer
        """
        if not server_response:
            self.last_failed = True
        else:
            self.waiting = False
            self.last_failed = False
    

    def next_tnx(self):
        """
        Generate the next transaction submitted by this peer
        - if previously submitted transaction failed, resubmit
        - if waiting for previous response, do not submit a new transaction
        - else, if previous tnx is read, submit a write request with the same key
            otherwise, submit a read request with a new key
        """
        if self.last_failed:
            return self.tnx_history[-1]
        if self.waiting:
            return None
        if len(self.tnx_history) > 0 and self.tnx_history[-1].tnx_type == 'read':
            new_tnx = transaction(self.name, 'write', self.tnx_history[-1].key)
        else:
            new_tnx = transaction(self.name, 'read', self.workload_generator.generate())
        self.waiting = True
        self.tnx_history.append(new_tnx)
        return new_tnx


class write_peer(peer):
    """
    Peer that only submits write requests following a given distribution
    """

    def __init__(self, name: str, write_keygen: workload_generator):
        self.name = name
        self.write_keygen = write_keygen
        self.last_failed = False
        self.waiting = False
        self.last_submitted_txn = None
    

    def check_if_next_transaction(self):
        return True
    

    def update_peer_state(self, server_response):
        self.last_failed = server_response

    
    def next_tnx(self):
        """
        Resubmit if previous tnx failed
        """
        if self.last_failed:
            return self.last_submitted_txn
        else:
            key = self.write_keygen.generate()
            self.last_submitted_txn = transaction(self.name, 'write', key)
            return self.last_submitted_txn


class rw_peer(peer):
    """
    Peer with different read/write access pattern
    """

    def __init__(self, name: str, read_prob: float,
                 read_keygen: workload_generator, write_keygen: workload_generator):
        self.name = name
        self.read_prob = read_prob
        self.read_keygen = read_keygen
        self.write_keygen = write_keygen
        self.last_failed = False
        self.last_submitted_txn = None


    def check_if_next_transaction(self):
        return True
    

    def update_peer_state(self, server_response):
        self.last_failed = server_response
    

    def next_tnx(self):
        """
        Generate the next transaction
        - if previous transaction failed, resubmit
        - otherwise, generate a new transaction based on specified r/w ratio and distribution
        """
        if self.last_failed:
            assert(self.last_submitted_txn is not None)
            return self.last_submitted_txn
        else:
            prob = np.random.random()
            action, generate = "write", self.write_keygen
            if prob < self.read_prob:
                action, generate = "read", self.read_keygen
        
            key = generate.generate()
            self.last_submitted_txn = transaction(self.name, action, key)
            return self.last_submitted_txn
