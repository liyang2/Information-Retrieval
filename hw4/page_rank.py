from __future__ import division
import numpy as np
from scipy.sparse import csc_matrix
from numpy import linalg as LA

import itertools
from collections import defaultdict

result_file = 'PR_result_test.txt'
inlinks_file = 'wt2g_inlinks.txt'
# inlinks_file = 'inlinks.txt'
# inlinks_file = 'test_inlinks.txt'
max_rounds = 500  # how many rounds are used to iteratively calculate page rank
c = 0.8
threshold = 1e-8


def build_matrix():
    counter = itertools.count()
    outlink_count = defaultdict(set)

    global node_lst, index

    # first read file
    with open(inlinks_file) as f:
        f_lines = f.readlines()
        # first round
        # give each node an integer index in the matrix
        for line in f_lines:
            line = line.split()
            node, inlinks = line[0], line[1:]
            node_lst.append(node)
            index[node] = next(counter)
            for inlink in inlinks:
                outlink_count[inlink].add(node)

        data, row, col = [], [], []
        # second round: construct matrix
        for line in f_lines:
            line = line.split()
            row_num = index[line[0]]
            in_links_for_each_node = set()
            for inlk in line[1:]:
                if inlk in in_links_for_each_node:
                    continue
                else:
                    in_links_for_each_node.add(inlk)

                col_num = index[inlk]
                row.append(row_num)
                col.append(col_num)
                data.append(1 / len(outlink_count[inlk]))
        print "size is ", len(index)
        sparse_matrix = csc_matrix((np.array(data), (np.array(row), np.array(col))),
                                   shape=(len(index), len(index)))
    return sparse_matrix


def calculate_pr(sparse_matrix):
    global node_lst
    pr = initial = np.asmatrix(np.full((sparse_matrix.shape[0], 1), 1 / sparse_matrix.shape[0]))
    for i in range(max_rounds):
        t = pr
        pr = c * sparse_matrix * pr + (1-c) * initial
        print LA.norm(pr - t, 1)
        if LA.norm(pr - t, 1) < threshold:
            print "Breaking at {}".format(i)
            break
    zipped = zip(node_lst, pr.A1)
    zipped = sorted(zipped, key=lambda x:x[1], reverse=True)
    with open(result_file, 'w') as f:
        for item in zipped:
            f.write(item[0] + " " + str(item[1]) + "\n")


def check_file_validity():
    nodes = set()
    with open(inlinks_file) as f:
        for line in f:
            parts = line.split()
            for p in parts:
                nodes.add(p)
    print "size of nodes:", len(nodes)


if __name__ == '__main__':
    # check_file_validity()
    node_lst = []
    index = {}  # node : index
    A = build_matrix()

    calculate_pr(A)




