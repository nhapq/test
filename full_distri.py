from array import array
from hashlib import md5
from random import shuffle
from struct import unpack_from
from time import time


class Ring(object):
    # Non-PEP8 formatting to make blog formatting happy.
    def __init__(self, nodes, part2node, replicas):
        self.nodes = nodes
        self.part2node = part2node
        self.replicas = replicas
        partition_power = 1
        while 2 ** partition_power < len(part2node):
            partition_power += 1
        if len(part2node) != 2 ** partition_power:
            raise Exception("part2node's length is not an exact power of 2")
        self.partition_shift = 32 - partition_power
    def get_nodes(self, data_id):
        data_id = str(data_id)
        part = unpack_from('>I', md5(data_id).digest())[0] >> self.partition_shift
        node_ids = [self.part2node[part]]
        zones = [self.nodes[node_ids[0]]]
        for replica in xrange(1, self.replicas):
            while self.part2node[part] in node_ids and \
                   self.nodes[self.part2node[part]] in zones:
                part += 1
                if part >= len(self.part2node):
                    part = 0
            node_ids.append(self.part2node[part])
            zones.append(self.nodes[node_ids[-1]])
        return [self.nodes[n] for n in node_ids]


def build_ring(nodes, partition_power, replicas):
    begin = time()
    parts = 2 ** partition_power
    total_weight = float(sum(n['weight'] for n in nodes.itervalues()))
    for node in nodes.itervalues():
        node['desired_parts'] = parts / total_weight * node['weight']
    part2node = array('H')
    for part in xrange(2 ** partition_power):
        for node in nodes.itervalues():
            if node['desired_parts'] >= 1:
                node['desired_parts'] -= 1
                part2node.append(node['id'])
               # part2node.append(part % len(nodes))
                break
        else:
            for node in nodes.itervalues():
                if node['desired_parts'] >= 0:
                    node['desired_parts'] -= 1
                    part2node.append(node['id'])
                #    part2node.append(part % len(nodes))
                    break
    shuffle(part2node)
    ring = Ring(nodes, part2node, replicas)
#    print '%.02fs to build ring' % (time() - begin)
    return (ring,part2node)


def rebuild_ring(nodes, partition_power, replicas, old_part2node, old_nodes):
    parts = 2 ** partition_power
    total_weight = float(sum(n['weight'] for n in nodes.itervalues()))
    for node in nodes.itervalues():
        node['desired_parts'] = parts / total_weight * node['weight']
    part2node = array('H')
    for part in xrange(2 ** partition_power):
        for node in nodes.itervalues():
            if node['desired_parts'] >= 1:
                node['desired_parts'] -= 1
                part2node.append(node['id'])
                break
        else:
            for node in nodes.itervalues():
                if node['desired_parts'] >= 0:
                    node['desired_parts'] -= 1
                    part2node.append(node['id'])
                    break
#    print part2node
    node_ids = [node_id['id'] for node_id in nodes.itervalues()]
    old_node_ids = [node_id['id'] for node_id in old_nodes.itervalues()]
    if len(node_ids) > len(old_node_ids):
        for i in old_node_ids:
            diff =  old_part2node.count(i) - part2node.count(i)
            print diff
            print "try to replace randomly"
            random_replace_id(old_part2node, i, node_ids[-1], diff)

    ring = Ring(nodes, old_part2node, replicas)
#    print '%.02fs to build ring' % (time() - begin)
    return ring


def random_replace_id(data_list, id_1, id_2, repeat_time=1):
    import random
    time_count = 0 
    if data_list != []:
        position_id = [i for i,x in enumerate(data_list) if x == id_1]
        while time_count < repeat_time:
            pos = random.choice(position_id)
            elem = data_list[pos]
            if elem == id_1:
                data_list[pos] = id_2
                time_count += 1
        return data_list
    else:
        return None

total_size = 0
object_list = []
with open('/root/file_list.txt', 'a+') as f:
    for line in f.readlines():
        if 'mp4' in line:
            object = {}
            object['size'] = int(str(line).split('/mnt')[0].strip()) / 1024
            object['path'] = '/mnt' + str(line).split('/mnt')[1]
            object_list.append(object)
            total_size += object['size']



def test_ring(ring):
    begin = time()
    DATA_ID_COUNT = len(object_list)
    node_counts = {}
    node_sizes = {}
    zone_counts = {}
    location = {}
    for data in object_list:
        location[data['path']] = []
        for node in ring.get_nodes(data['path']):
            location[data['path']].append(node['id'])
            node_counts[node['id']] = node_counts.get(node['id'], 0) + 1
            node_sizes[node['id']] = node_sizes.get(node['id'], 0) + data['size']
#    print '%ds to test ring' % (time() - begin)
    total_weight = float(sum(n['weight'] for n in ring.nodes.itervalues()))
    max_over = 0
    max_under = 0
    for node in ring.nodes.itervalues():
        desired = DATA_ID_COUNT * REPLICAS * node['weight'] / total_weight
        diff = node_counts[node['id']] - desired
        if diff > 0:
            over = 100.0 * diff / desired
            if over > max_over:
                max_over = over
        else:
            under = 100.0 * (-diff) / desired
            if under > max_under:
                max_under = under

    max_over_capa = 0
    max_under_capa = 0
    for node in ring.nodes.itervalues():
        desired_capa = total_size * REPLICAS * node['weight'] / total_weight
        diff_capa = node_sizes[node['id']] - desired_capa
        if diff_capa > 0:
            over_capa = 100.0 * diff_capa / desired_capa
            if over_capa > max_over_capa:
                max_over_capa = over_capa
        else:
            under_capa = 100.0 * (-diff_capa) /desired_capa
            if under_capa > max_under_capa:
                max_under_capa = under_capa


    print 'the total size is: %d MB' % total_size
    print 'the total files is: %d' % DATA_ID_COUNT
    print 'Number of nodes: %d' % NODE_COUNT
    print 'Number of replicas per file: %d' %REPLICAS
    print 'the weight ratio is: 1-2-1-2 (2 for odd nodes, 1 for even nodes)'

    print 'the max over the desired file_per_node is %.02f%%' % max_over
    print 'the max under the desired file_per_node is %.02f%%' % max_under
    print 'the max over the desired capacity is %.02f%%' % max_over_capa
    print 'the max under the desired capacity is %.02f%%' % max_under_capa
    print 'the distributed files per node: %s' % str(node_counts)
    print 'the distributed size per node: %s' % str(node_sizes)
#    for data in object_list:
#        for node in ring.get_nodes(data['path']):
#        compare = ring.get_nodes(data['path'])
#        if compare[0]['id'] == compare[1]['id']:
#            print compare
    return location

if __name__ == '__main__':
    PARTITION_POWER = 16
    REPLICAS = 2
    NODE_COUNT = 4
    ZONE_COUNT = 1
    nodes = {}
    while len(nodes) < NODE_COUNT:
        zone = 0
        while zone < ZONE_COUNT and len(nodes) < NODE_COUNT:
            node_id = len(nodes)
            nodes[node_id] = {'id': node_id, 'zone': zone,
                              'weight': 1.0 + (node_id % 2)}
            zone += 1
    (ring,old_part2node) = build_ring(nodes, PARTITION_POWER, REPLICAS)
    location1 = test_ring(ring)

    print 'OK, now we add a new node and rebuilt the ring'
    NODE_COUNT = 5

    nodes2 = {}
    while len(nodes2) < NODE_COUNT:
        zone = 0
        while zone < ZONE_COUNT and len(nodes) < NODE_COUNT:
            node_id = len(nodes2)
            nodes2[node_id] = {'id': node_id, 'zone': zone,
                              'weight': 1.0 + (node_id % 2)}
            zone += 1
    ring2 = rebuild_ring(nodes2, PARTITION_POWER, REPLICAS, old_part2node, nodes)
    location2= test_ring(ring2)

    move_number = 0
    for data in object_list:
        lo1 = location1.get(data['path'])
        lo2 = location2.get(data['path'])
        for j in lo2:
            if j not in lo1:
                move_number += 1
    print move_number

