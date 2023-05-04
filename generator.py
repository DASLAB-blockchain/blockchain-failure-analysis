import numpy as np
from abc import ABC, abstractmethod


class workload_generator(ABC):
    """
    Abstract workload key generator class
    """

    @abstractmethod
    def generate(self, size=1):
        pass


class zipfian_generator(workload_generator):
    """
    Basic zipfian key generator
    https://stackoverflow.com/questions/57413721/how-can-i-generate-random-variables-using-np-random-zipf-for-a-given-range-of-va
    """

    def __init__(self, lo, hi, a):
        """
        Range of samples are from [lo, hi] with zipfian distribution a
        """
        self.keys = np.linspace(lo, hi, hi - lo + 1)
        self.probs = a ** np.negative(self.keys)
        self.probs /= np.sum(self.probs)
    

    def generate(self, size=1):
        """
        Sample with replacement with initialized prob distribution
        """
        return np.random.choice(self.keys, size=size, replace=True, p=self.probs)
