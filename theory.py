"""
Compute theoretical success rate for given blockchain setting
"""


def compute_success_rate(
        block_size,
        read_keys, read_key_probs,
        write_keys, write_key_probs,
        read_frac, write_frac
):
    """
    Compute the theoretical conflict probability of two given keys
    Assumes all peers have identical access patterns
    """
    
    # Let (a, b) be two keys in the block, with b behind a
    # Compute (a, b)'s conflict rate

    # construct a dictionary from key to probability
    action_dict = {'read': read_frac, 'write': write_frac}
    prob_dict = {'read': dict(), 'write': dict()}
    for key, prob in zip(read_keys, read_key_probs):
        prob_dict['read'][key] = prob
    for key, prob in zip(write_keys, write_key_probs):
        prob_dict['write'][key] = prob
    
    conflict_prob = 0
    for a_action in ['read', 'write']:
        for b_action in ['read', 'write']:

            # Read-read: no conflict
            # otherwise: compute conflict
            if not (a_action == 'read' and b_action == 'read'):
                
                # iterate through b's possible values, and
                # compute its conflict rate with a
                for bval, bprob in prob_dict[b_action].items():
                    aprob = prob_dict[a_action].get(bval, 0)
                    conflict_prob += bprob * aprob * action_dict[a_action] * action_dict[b_action]
    
    # Now, compute the expected conflict probbaility within the block
    # Let {0, 1, 2, ..., block_size - 1} be all transactions within the block
    # Then, for keys 1...block_size - 1, the probability of conflict is
    # 1 - Prob[no conflict with all previous keys]
    expected_conflict_count = 0
    for i in range(1, block_size):
        expected_conflict_count += 1 - (1 - conflict_prob)**i
    
    return (block_size - expected_conflict_count) / block_size


