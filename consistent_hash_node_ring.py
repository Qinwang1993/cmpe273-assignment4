import pickle
import hashlib

class ConsistentHashNodeRing():
    def __init__(self, nodes, replicas):
        self.replicas = replicas
        self.ring = dict()
        self.sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        for i in range(0, self.replicas):
            key = self.gen_key("%s:%s" % (node, i))
            self.ring[key] = node
            self.sorted_keys.append(key)

    def remove_node(self, node):
        for i in range(0, self.replicas):
            key = self.gen_key("%s:%s" % (node, i))
            del self.ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, str):
        if not self.ring:
            return None
        key = self.gen_key(str)
        keys = self.sorted_keys
        for i in range(0, len(keys)):
            cur = keys[i]
            if key <= cur:
                return self.ring[cur]
        return self.ring[keys[0]]


    def gen_key(self, key):
        return int(hashlib.md5(pickle.dumps(key)).hexdigest(), 16)