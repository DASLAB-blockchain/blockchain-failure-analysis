import numpy as np
from abc import ABC, abstractmethod


class workload_generator(ABC):
    """
    Abstract workload key generator class
    """

    @abstractmethod
    def generate(self):
        pass


class zipfian_generator(workload_generator):
    """
    Basic zipfian key generator
    https://stackoverflow.com/questions/57413721/how-can-i-generate-random-variables-using-np-random-zipf-for-a-given-range-of-va
    """

    @staticmethod
    def compute_keys_and_probs(lo, hi, a, reverse):
        keys = np.linspace(lo, hi, hi - lo + 1)
        probs = a ** np.negative(keys)
        probs /= np.sum(probs)
        if reverse:
            probs = np.flip(probs)
        return keys, probs


    def __init__(self, lo, hi, a, reverse):
        """
        Range of samples are from [lo, hi] with zipfian distribution a
        if reverse, zipfian pmf is reversed (hot keys are larger)
        """
        self.keys, self.probs = self.compute_keys_and_probs(lo, hi, a, reverse)
    

    def generate(self):
        """
        Sample with replacement with initialized prob distribution
        """
        return np.random.choice(self.keys, replace=True, p=self.probs)
