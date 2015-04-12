from __future__ import division
import numpy as np
from scipy.sparse import csc_matrix
from numpy import linalg as LA

import itertools
from collections import defaultdict

result_file = 'PR_result_2.txt'
inlinks_file = 'wt2g_inlinks.txt'
inlinks_file = 'inlinks.txt'
# inlinks_file = 'test_inlinks.txt'
max_rounds = 100  # how many rounds are used to iteratively calculate page rank
c = 1
threshold = 1e-8


def build_matrix():
    counter = itertools.count()
    outlink_count = defaultdict(lambda: 0)

    global node_lst, index

    # first read file
    with open(inlinks_file) as f:
        f = f.readlines()
        # first round
        # give each node an integer index in the matrix
        for line in f:
            line = line.split()
            node, inlinks = line[0], line[1:]
            node_lst.append(node)
            index[node] = next(counter)
            for inlink in inlinks:
                outlink_count[inlink] += 1

        data, row, col = [], [], []
        # second round: construct matrix
        for line in f:
            line = line.split()
            row_num = index[line[0]]
            for inlk in line[1:]:
                if inlk not in index:
                    continue
                col_num = index[inlk]
                row.append(row_num)
                col.append(col_num)
                data.append(1 / outlink_count[inlk])

        sparse_matrix = csc_matrix((np.array(data), (np.array(row), np.array(col))),
                                   shape=(len(index), len(index)))
    return sparse_matrix


def calculate_pr(sparse_matrix):
    global node_lst
    pr = initial = np.asmatrix(np.full((A.shape[0], 1), 1/A.shape[0]))
    for i in range(max_rounds):
        t = pr
        pr = c * sparse_matrix * pr + (1-c) * initial
        pr /= pr.sum()  # normalize
        if LA.norm(pr - t, 1) < threshold:
            print "Breaking at {}".format(i)
            break
    zipped = zip(node_lst, pr.A1)
    zipped = sorted(zipped, key=lambda x:x[1], reverse=True)
    with open(result_file, 'w') as f:
        for item in zipped:
            f.write(item[0] + " " + str(item[1]) + "\n")



if __name__ == '__main__':
    node_lst = []
    index = {}  # node : index
    A = build_matrix()
    calculate_pr(A)



