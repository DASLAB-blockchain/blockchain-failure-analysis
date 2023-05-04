import random
import simulator, generator


def rw_experiment(
        num_ops,
        block_size,
        n_peers,
        key_range_min,
        key_range_max,
        zipfian_alpha
    ):
    block_server = simulator.block_server(block_size)
    peers = list()
    name2idx = dict()
    for idx in range(n_peers):
        gen = generator.zipfian_generator(
            key_range_min, key_range_max, zipfian_alpha)
        peer_name = "Peer{}".format(idx)
        peers.append(simulator.simple_rw_peer(peer_name, gen))
        name2idx[peer_name] = idx
    
    block_history = list()
    success_history = list()

    while block_server.processed_op_count < num_ops:

        # try to process a simulated block
        block, success = block_server.process_block()
        if block is not None and success is not None:
            block_history.append(block)
            success_history.append(success)

            # change the waiting status for processed peers
            for tnx in block:
                peers[name2idx[tnx.user]].flip_state()


        # simulated peers generate more pending requests
        new_tnxs = list()
        for peer in peers:
            new_tnx = peer.next_tnx()
            if new_tnx is not None:
                new_tnxs.append(new_tnx)
        
        # shuffle current pending transactions and add to simulated block server
        random.shuffle(new_tnxs)
        block_server.accept_new_tnxs(new_tnxs)
    
    return block_history, success_history, block_server.processed_op_count, block_server.success_op_count
