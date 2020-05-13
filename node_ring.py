import hashlib
import mmh3

from server_config import NODES


class NodeRing():

    def __init__(self, nodes, seed = 0):
        assert len(nodes) > 0
        self.nodes = nodes
        self.seed = seed
        self.hash = lambda x: mmh3.hash(x, seed)
    
    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = key % len(self.nodes)
        return self.nodes[node_index]

    def get_node_RHW_hash(self, key_hex):
        largest = -1
        res = None
        for node in self.nodes:
            weight = self.hash("%s-%s" % (str(node), key_hex))
            if weight > largest:
                largest = weight
                res = node
            elif weight == largest:
                res = max(str(node), str(res))
        return res


def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
# test()
