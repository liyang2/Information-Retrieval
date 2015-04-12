#!/usr/bin/python
from __future__ import division
import sys
from collections import defaultdict
from math import log

class TrecEvalResult:
    def __init__(self, query_id):
        self.query_id = query_id
        # key is in {5, 10, 20, 50, 100}
        self.prec = {}  # precision value after k docs retrieved
        self.recl = {}  # recall value after k docs retrieved
        self.f1 = {}  # f1 value after k docs retrieved

        for k in [5, 10, 20, 50, 100]:
            self.prec[k] = 0
            self.recl[k] = 0
            self.f1[k] = 0

        self.r_prec = 0
        self.avg_prec = 0
        self.nDCG = 0

        self.total_rel_c = 0
        self.prec2 = []  # precision values after each relevant doc retrieved


def main(argv):
    with_dash_q = False
    if len(argv) == 3:
        with_dash_q = False
        qrel_file = argv[1]
        results_file = argv[2]
    elif len(argv) == 4 and argv[1] == '-q':
        with_dash_q = True
        qrel_file = argv[2]
        results_file = argv[3]
    else:
        print "Incorrect arguments"
        return


    qrels = defaultdict(dict)
    results = defaultdict(list)
    trec_eval_dict = {}  # query_id : TrecEvalResult object

    # read file into memory
    with open(results_file) as f:
        for line in f:
            parts = line.split(" ")
            # (query_id, doc_id, rank, score)
            results[parts[0]].append((parts[0], parts[2], int(parts[3]), float(parts[4])))
            query_id = parts[0]
            if query_id not in trec_eval_dict:
                trec_eval_dict[query_id] = TrecEvalResult(query_id)

    with open(qrel_file) as f:
        for line in f:
            parts = line.split(" ")
            # (query_id, assessor_id, doc_id, grade)
            qrels[parts[0]][parts[2]] = (parts[0], parts[1], parts[2], int(parts[3]))
            query_id, grade = parts[0], int(parts[3])
            if query_id in trec_eval_dict and grade > 0:
                trec_eval_dict[query_id].total_rel_c += 1

    def rel_score(query_id, doc_id):
        assert query_id in qrels
        if doc_id in qrels[query_id] and qrels[query_id][doc_id][-1] > 0:
            return qrels[query_id][doc_id][-1]
        else:
            return 0

    # start evaluating
    ks = set([5, 10, 20, 50, 100])
    for query_id, lst in results.iteritems():
        ret_rel_c = 0  # retrieved and relevant count
        assert query_id in qrels
        # sort according to score
        lst.sort(key=lambda x: x[-1], reverse=True)
        for i, doc_tuple in enumerate(lst):
            query_id_, doc_id, rank, score = doc_tuple
            if rel_score(query_id, doc_id) > 0:  # grade > 0
                ret_rel_c += 1
                trec_eval_dict[query_id].prec2.append(ret_rel_c / (i+1))
            if i + 1 in ks:
                trec_eval_dict[query_id].prec[i+1] = f1_prec = ret_rel_c / (i+1)
                trec_eval_dict[query_id].recl[i+1] = f1_recal = ret_rel_c / trec_eval_dict[query_id].total_rel_c
                trec_eval_dict[query_id].f1[i+1] = 2 * f1_prec * f1_recal / (f1_prec + f1_recal) \
                    if f1_prec > 0 and f1_recal > 0 else 0
            if i+1 == trec_eval_dict[query_id].total_rel_c:
                trec_eval_dict[query_id].r_prec = ret_rel_c / (i+1)

        # avg-Precision
        # note that it's very likely that 'results' doesn't contain all relevant docs
        prec2 = trec_eval_dict[query_id].prec2
        trec_eval_dict[query_id].avg_prec = sum(prec2) / trec_eval_dict[query_id].total_rel_c
        if len(lst) < trec_eval_dict[query_id].total_rel_c:
            # retrieved docs # < total relevant docs #
            trec_eval_dict[query_id].r_prec = ret_rel_c / trec_eval_dict[query_id].total_rel_c

        # nDCG: Normalized Discounted Cumulative Gain
        def dcg_score(lst):
            query_id, doc_id = lst[0][0], lst[0][1]
            dcg = rel_score(query_id, doc_id)
            for i, tup in enumerate(lst[1:]):
                query_id, doc_id = tup[0], tup[1]
                dcg += rel_score(query_id, doc_id) / log(i+2, 2)
            return dcg

        ideal_lst = sorted(lst, key=lambda x: rel_score(x[0], x[1]), reverse=True)
        deno = dcg_score(ideal_lst)
        if deno > 0:
            trec_eval_dict[query_id].nDCG = dcg_score(lst) / deno



    def print_single_result(obj):
        print "Query id: {}".format(obj.query_id)
        print "{0:3s}  {1:6s}  {2:6s}  {3:6s}".format("k", "prec", "recl", "f1")
        for k in [5, 10, 20, 50, 100]:
            print "{0:3d}  {1:.4f}  {2:.4f}  {3:.4f}".format(k, obj.prec[k], obj.recl[k], obj.f1[k])
        print "R-precision: {:.4f}".format(obj.r_prec)
        print "Average precision: {:.4f}".format(obj.avg_prec)
        print "nDCG: {:.4f}".format(obj.nDCG)
        print ""

    # print result
    if with_dash_q:
        for obj in sorted(trec_eval_dict.itervalues(), key=lambda x: x.query_id):
            print_single_result(obj)

    sum_obj = TrecEvalResult(-1)
    for obj in trec_eval_dict.itervalues():
        for k in [5, 10, 20, 50, 100]:
            sum_obj.prec[k] += obj.prec[k]
            sum_obj.recl[k] += obj.recl[k]
            sum_obj.f1[k] += obj.f1[k]
        sum_obj.r_prec += obj.r_prec
        sum_obj.avg_prec += obj.avg_prec
        sum_obj.nDCG += obj.nDCG

    for k in [5, 10, 20, 50, 100]:
        sum_obj.prec[k] /= len(trec_eval_dict)
        sum_obj.recl[k] /= len(trec_eval_dict)
        sum_obj.f1[k] /= len(trec_eval_dict)
    sum_obj.r_prec /= len(trec_eval_dict)
    sum_obj.avg_prec /= len(trec_eval_dict)
    sum_obj.nDCG /= len(trec_eval_dict)
    sum_obj.query_id = len(trec_eval_dict)
    print_single_result(sum_obj)



if __name__ == '__main__':
    main(sys.argv)