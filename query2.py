
# query.py used elastic search for indexing
# this file instead use index built by my own

from __future__ import division
from collections import defaultdict
import query
import os
import re
import indexing
from indexing import modes
from stemming.porter2 import stem

def get_doc_info():
    doc_inf = {} # doc_id: {doc_no: 'xxx', doc_len: 'xxx'}
    total_doc_len = 0
    with open('index/doc_info.txt', 'r') as f:
        for line in f:
            doc_no, doc_id, doc_len = line.rstrip('\n').split(' ')
            doc_id, doc_len = int(doc_id), int(doc_len)
            doc_inf[doc_id] = {'doc_no': doc_no, 'doc_len': doc_len}
            total_doc_len += doc_len
    return doc_inf, total_doc_len

def get_term_offsets(mode):
    term_offset ={} # term: (beginOffset, endOffset)
    with open('index/{}/cache.txt'.format(mode)) as f:
        for line in f:
            term, beginOffest, endOffset = line.rstrip('\n').split(' ')
            beginOffest, endOffset = int(beginOffest), int(endOffset)
            term_offset[term] = (beginOffest, endOffset)
    return term_offset


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
        terms = map(stem, indexing.stopping_filter(terms, stopwords))
    return query_no, terms


def read_index_entry(term, mode):
    global term_offsets
    index_entry = {'term': term, 'df': 0, 'ttf': 0, 'hits': {}}
    term, df, ttf, d_blocks = indexing.get_index_entry_parts(indexing.read_file('index/{}/index_1.txt'.format(mode), term_offsets[term]))
    index_entry['df'], index_entry['ttf'] = int(df), int(ttf)
    i = 0
    while i < len(d_blocks):
        doc_id, tf = int(d_blocks[i]), int(d_blocks[i+1])
        index_entry['hits'][doc_id] = []
        i, j = i+2, 0
        for j in xrange(tf):
            index_entry['hits'][doc_id].append(int(d_blocks[i+j]))
        i += tf
    return index_entry


def compute_scores(query_no, query_terms, mode):
    assert mode in modes
    global doc_info, term_offsets, avg_length, total_docs, voc_size
    okapi_tf_file = 'results2/{}/okapi_tf.txt'.format(mode)
    okapi_bm25_file = 'results2/{}/okapi_bm25.txt'.format(mode)
    unigram_lm_laplace_file = 'results2/{}/unigram_lm_laplace.txt'.format(mode)

    okapi_tf_scores = defaultdict(lambda: 0.0)
    okapi_bm25_scores = defaultdict(lambda : 0.0)
    unigram_lm_laplace_scores = defaultdict(lambda : 0.0)

    for q_term in query_terms:
        if q_term not in term_offsets:
            continue
        print q_term,
        entry = read_index_entry(q_term, mode)
        for doc_id in doc_info:
            doc_number = doc_info[doc_id]['doc_no']
            doc_len = doc_info[doc_id]['doc_len']
            if doc_id in entry['hits']:
                pos_arr = entry['hits'][doc_id]
                tf = len(pos_arr)
            else:
                tf = 0
            okapi_tf_scores[doc_number] += query.okapi_tf(tf, doc_len, avg_length)
            okapi_bm25_scores[doc_number] += \
                query.okapi_bm25(tf, query_terms.count(q_term), doc_len,
                                 entry['df'], 1.2, 100, 0.75, total_docs, avg_length)
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

def remove_previous_results(directory):
    for subdir, dirs, files in os.walk(directory):
        for file in files:
            os.remove(os.path.join(subdir, file))


if __name__ == '__main__':
    mode = 'stemming'
    remove_previous_results('results2/{}'.format(mode))
    query_file = 'AP_DATA/query_desc.51-100.short.txt'
    tokenize_regex = r"[0-9A-Za-z]+\w*(?:\.?\w+)*"
    index = {}
    doc_info, total_doc_len = get_doc_info()
    term_offsets = get_term_offsets(mode)
    total_docs = len(doc_info)
    avg_length = total_doc_len / total_docs
    voc_size = len(term_offsets)

    stopwords = indexing.load_stopwords()
    print "avg_length: " + str(avg_length)
    print "voc_size: " + str(voc_size)
    print "total_docs: " + str(total_docs)

    with open(query_file) as f:
        for line in f:
            if line.strip():
                query_no, terms = analyze_query(line, tokenize_regex, mode)
                print "\nquery_no: " + query_no+" :",
                compute_scores(query_no, terms, mode)