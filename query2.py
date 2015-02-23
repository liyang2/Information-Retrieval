
# query.py used elastic search for indexing
# this file instead use index built by my own

from __future__ import division
from collections import defaultdict
from struct import *
import query
import os
import re
import indexing
from indexing import modes
from stemming.porter2 import stem

def recover(mode):
    assert mode in modes
    global df_counts, ttf_counts, id_to_term, id_to_doc_number, doc_lens, index
    global total_docs, avg_length, voc_size
    # id_to_doc_number, doc_lens
    with open('index/{}/doc_id_mappings.txt'.format(mode), 'r') as f:
        sum_doc_len = 0
        for line in f:
            line = line.rstrip('\n')
            doc_number, doc_id, doc_len = line.split(" ")
            id_to_doc_number[int(doc_id)] = doc_number
            doc_lens[int(doc_id)] = int(doc_len)
            sum_doc_len += int(doc_len)
    total_docs = len(doc_lens)
    avg_length = sum_doc_len / total_docs
    # id_to_term
    with open('index/{}/term_id_mappings.txt'.format(mode), 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            term, term_id = line.split(" ")
            id_to_term[int(term_id)] = term
            index[term] = {}
    voc_size = len(id_to_term)
    # df_counts, ttf_counts, index
    with open('index/{}/index.txt'.format(mode), 'rb') as f:
        while True:
            str = f.read(12)
            if str == "":
                break
            term_id, df, ttf = unpack("=3L", str)
            term = id_to_term[term_id]
            df_counts[term] = df
            ttf_counts[term] = ttf
            for i in range(df): # how many docs
                doc_id, tf = unpack("=LH", f.read(6))
                pos_arr = unpack("="+`tf`+"H", f.read(tf * 2))
                index[term][doc_id] = pos_arr

def analyze_query(line, tokenize_regex, mode):
    assert mode in modes
    global stopwords
    query_no = line.split(".")[0]
    terms_part = line[line.find(".")+1:]
    terms = re.findall(tokenize_regex, terms_part)
    terms = [term.lower() for term in terms[3:]]
    if mode == 'stemming':
        terms = map(stem, terms)
    elif mode == 'stopping':
        terms = indexing.stopping_filter(terms, stopwords)
    elif mode == 'both':
        terms = indexing.stem_filter(indexing.stopping_filter(terms, stopwords))
    return query_no, terms


def compute_scores(query_no, query_terms, mode):
    assert mode in modes
    global index, df_counts, ttf_counts, doc_lens, avg_length, total_docs, voc_size
    okapi_tf_file = 'results2/{}/okapi_tf.txt'.format(mode)
    okapi_bm25_file = 'results2/{}/okapi_bm25.txt'.format(mode)
    unigram_lm_laplace_file = 'results2/{}/unigram_lm_laplace.txt'.format(mode)

    okapi_tf_scores = defaultdict(lambda: 0.0)
    okapi_bm25_scores = defaultdict(lambda : 0.0)
    unigram_lm_laplace_scores = defaultdict(lambda : 0.0)

    for q_term in query_terms:
        if not index.has_key(q_term):
            continue
        print q_term,
        for doc_id in doc_lens:
            doc_number = id_to_doc_number[doc_id]
            doc_len = doc_lens[doc_id]
            if index[q_term].has_key(doc_id):
                pos_arr = index[q_term][doc_id]
                tf = len(pos_arr)
            else:
                tf = 0
            okapi_tf_scores[doc_number] += query.okapi_tf(tf, doc_len, avg_length)
            okapi_bm25_scores[doc_number] += \
                query.okapi_bm25(tf, query_terms.count(q_term), doc_len,
                                 df_counts[q_term], 1.2, 100, 0.75, total_docs, avg_length)
            unigram_lm_laplace_scores[doc_number] += \
                query.unigram_lm_laplace(tf, doc_len, voc_size)
    okapi_tf_result = query.get_top_k_docs(okapi_tf_scores, 100)
    okapi_bm25_result = query.get_top_k_docs(okapi_bm25_scores, 100)
    unigram_lm_laplace_result = query.get_top_k_docs(unigram_lm_laplace_scores, 100)

    for i in range(100):
        query.write_result(okapi_tf_file, query_no, okapi_tf_result[i][1], i+1, okapi_tf_result[i][0])
        query.write_result(okapi_bm25_file, query_no, okapi_bm25_result[i][1], i+1, okapi_bm25_result[i][0])
        query.write_result(unigram_lm_laplace_file, query_no, unigram_lm_laplace_result[i][1], i+1,
                     unigram_lm_laplace_result[i][0])

def remove_previous_results(mode):
    dir = os.path.dirname(__file__) + "/results2/{}".format(mode)
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            os.remove(os.path.join(subdir, file))


if __name__ == '__main__':
    mode = 'stemming'
    remove_previous_results(mode)
    query_file = 'AP_DATA/query_desc.51-100.short.txt'
    tokenize_regex = r"[0-9A-Za-z]+\w*(?:\.?\w+)*"
    df_counts = {}
    ttf_counts = {}
    id_to_term = {}
    id_to_doc_number = {}
    doc_lens = {}
    index = {}
    avg_length = voc_size = total_docs = 0
    stopwords = indexing.load_stopwords()
    recover(mode)
    print "Recover() done!"
    print "avg_length: " + `avg_length`
    print "voc_size: " + `voc_size`
    print "total_docs: " + `total_docs`
    with open(query_file) as f:
        for line in f:
            if not line.startswith("\n"):
                query_no, terms = analyze_query(line, tokenize_regex, mode)
                print "\nquery_no: " + query_no+" :",
                compute_scores(query_no, terms, mode)