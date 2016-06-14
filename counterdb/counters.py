import asyncio
from collections import defaultdict


"""
{
    "node-name": {
        "counter-key": 0
        "counter-key-rps": 0
    }
}
"""


class CounterManager(object):
    def __init__(self, name):
        self.counters = defaultdict(lambda: defaultdict(int))
        self.name = name

    def increment(self, key):
        self.counters[self.name][key] += 1

    def get(self, key):
        """
        Get the total counter value for the given key. This just runs a sum
        acrossed the counter values for all other nodes
        """
        return sum([node[key] for node in self.counters.values() if key in node])

    def get_all(self):
        totals = defaultdict(int)
        for node in self.counters.values():
            for key, val in node.items():
                totals[key] += val
        return totals

    def to_dict(self):
        return self.counters

    def merge(self, other):
        for node, counters in other.items():
            for key, val in counters.items():
                if val > self.counters[node][key]:
                    self.counters[node][key] = val
