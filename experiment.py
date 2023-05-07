import random
import simulator, generator


def rw_experiment(
        num_ops,
        block_size,
        tnx_arrival_rate,
        n_peers,
        key_range_min,
        key_range_max,
        zipfian_alpha
    ):
    peers = list()
    for idx in range(n_peers):
        gen = generator.zipfian_generator(
            key_range_min, key_range_max, zipfian_alpha, False)
        peer_name = "Peer{}".format(idx)
        peers.append(simulator.simple_rw_peer(peer_name, gen))
    
    return do_experiment(num_ops, block_size, tnx_arrival_rate, peers)


def do_experiment(
        num_ops,
        block_size,
        tnx_arrival_rate,
        peers
    ):
    block_server = simulator.block_server(block_size)
    block_history = list()
    success_history = list()

    name2idx = dict()
    for idx, peer in enumerate(peers):
        name2idx[peer.name] = idx

    while block_server.processed_op_count < num_ops:

        # try to process a simulated block
        block, success = block_server.process_block()
        if block is not None and success is not None:
            block_history.append(block)
            success_history.append(success)

            # change the waiting status for processed peers
            for tnx, response in zip(block, success):
                peers[name2idx[tnx.user]].update_peer_state(response)

        # simulated peers generate more pending requests
        candidates = list()
        for idx, peer in enumerate(peers):
            if peer.check_if_next_transaction():
                candidates.append(idx)
        if tnx_arrival_rate == -1:
            selected_candidates = candidates
        else:
            budget = min(len(candidates), tnx_arrival_rate)
            selected_candidates = random.sample(candidates, k=budget)

        new_tnxs = list()
        for selected_idx in selected_candidates:
            new_tnxs.append(peers[selected_idx].next_tnx())
        
        # shuffle current pending transactions and add to simulated block server
        random.shuffle(new_tnxs)
        block_server.accept_new_tnxs(new_tnxs)
    
    return block_history, success_history, block_server.processed_op_count, block_server.success_op_count
