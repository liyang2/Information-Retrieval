from __future__ import division
from __future__ import print_function
import os
import json
import re
import math
from elasticsearch import Elasticsearch
import heapq
from collections import defaultdict

def avg_doc_length(es, index, doc_type):
    avg_length = es.search(index=index,doc_type=doc_type,body={
        "aggs" : {
            "avg_length" : {
                "avg" : {
                    "script": "doc['text'].values.size()"
                }
            }
        }
    })['aggregations']['avg_length']['value']
    return float(avg_length)


def vocabulary_size(es, index):
    search_result = es.search(index=index,doc_type='document',body={
            "aggs" : {
                "unique_terms" : {
                    "cardinality" : {"field": "text"}
                }
            }
        }
    )
    return int(search_result['aggregations']['unique_terms']['value'])


def tf_df(jsonObj):
    str = json.dumps(jsonObj)
    tf_pattern = re.compile(r"(?<=tf\(freq=)\d+\.\d+(?=\))")
    df_pattern = re.compile(r"(?<=idf\(docFreq=)\d+(?=,)")

    tf_match = tf_pattern.search(str)
    df_match = df_pattern.search(str, pos=tf_match.end(0))
    return float(tf_match.group(0)), float(df_match.group(0))

def doc_len(es, index, doc_id):
    len = es.search(index=index, doc_type='document', body={
        "query": {"match": {
            "_id": doc_id
        }},
        "aggs": {
            "doc_length":{
                "min":{
                    "script": "doc['text'].values.size()"
                }
            }
        }
    })['aggregations']['doc_length']['value']
    return int(len)


# return rows of information of docs which contains term
def docs_postings(es, index, term):
    global docs_info, total_docs
    print("term: " + term)
    if docs_info.has_key(term):
        return docs_info[term]
    else:
        results = es.search(
            index=index,
            doc_type='document',
            size=total_docs,
            explain=True, # so that we have tf, df info
            body={
                "query": {
                    "match": {
                        "text": term
                    }
                }
            }
        )
        # [ (id, (tf, df), doc_length) ]
        doc_postings = [
            (hit['_id'],
             tf_df(hit['_explanation']),
             doc_len(es, index, hit['_id'])
            ) for hit in results['hits']['hits']]
        docs_info[term] = doc_postings
        return doc_postings


def write_result(file_path, query_number, docno, rank, score):
    file = open(file_path, 'a')
    print("{query_number} Q0 {docno} {rank} {score} Exp".format(
        query_number=query_number,
        docno=docno,
        rank=rank,
        score=score), file=file)


def foo(tuple):
    return (-1)*tuple[0], tuple[1]


def get_top_k_docs(scores, k):
    pairs = [(-1 * score, id) for (id, score) in scores.iteritems()]
    heapq.heapify(pairs) # sorts by tuple[0]
    return [foo(heapq.heappop(pairs)) for i in range(k)]


def analyze_query(es, index, line):
    query_no = line.split(".")[0]
    if query_no[0].isdigit():
        modified = line[line.find(".")+1:]
        terms = es.indices.analyze(index=index, analyzer='my_english', text=modified)
        terms = [str(item['token']) for item in terms['tokens']]
        return query_no, terms[2:]

# Compute scores for a doc given a term
#   okapi_tf score
#   tf_idf score
#   okapi_bm23 score

def okapi_tf(tf, doc_len):
    global avg_length
    return tf / (tf + 0.5 + 1.5 * (doc_len/avg_length))

def tf_idf(tf, doc_len, df):
    global total_docs
    return okapi_tf(tf, doc_len) * math.log(total_docs / df)

def okapi_bm25(tf, doc_len, df, k1, k2, b):
    global total_docs, avg_length
    return math.log((total_docs + 0.5) / (df + 0.5)) * \
           ((tf + k1 * tf) / (tf + k1* ((1-b) + b * (doc_len / avg_length)))) * \
           ((tf + k2 * tf) / (tf + k2))

def unigram_lm_laplace(tf, doc_len):
    global voc_size
    return (tf + 1) / (doc_len + voc_size)


def compute_scores(es, index, query_no, query_terms):
    okapi_tf_file = os.path.dirname(__file__) + '/results/okapi_tf.txt'
    tf_idf_file = os.path.dirname(__file__) + '/results/tf_idf.txt'
    okapi_bm25_file = os.path.dirname(__file__) + '/results/okapi_bm25.txt'
    unigram_lm_laplace_file = os.path.dirname(__file__) + '/results/unigram_lm_laplace.txt'
    
    okapi_tf_scores = defaultdict(lambda: 0.0)
    tf_idf_scores = defaultdict(lambda: 0.0)
    okapi_bm25_scores = defaultdict(lambda : 0.0)
    unigram_lm_laplace_scores = defaultdict(lambda : 0.0)
    for q_term in query_terms:
        for id, (tf, df), doc_len in docs_postings(es, index, q_term):
            okapi_tf_scores[id] += okapi_tf(tf, doc_len)
            tf_idf_scores[id] += tf_idf(tf, doc_len, df)
            okapi_bm25_scores[id] += okapi_bm25(tf, doc_len, df, k1=1.2, k2=300, b=0.75)
            unigram_lm_laplace_scores[id] += unigram_lm_laplace(tf, doc_len)
    okapi_tf_result = get_top_k_docs(okapi_tf_scores, 100)
    tf_idf_result = get_top_k_docs(tf_idf_scores, 100)
    okapi_bm25_result = get_top_k_docs(okapi_bm25_scores, 100)
    unigram_lm_laplace_result = get_top_k_docs(unigram_lm_laplace_scores, 100)

    for i in range(100):
        write_result(okapi_tf_file, query_no, okapi_tf_result[i][1], i+1, okapi_tf_result[i][0])
        write_result(tf_idf_file, query_no, tf_idf_result[i][1], i+1, tf_idf_result[i][0])
        write_result(okapi_bm25_file, query_no, okapi_bm25_result[i][1], i+1, okapi_bm25_result[i][0])
        write_result(unigram_lm_laplace_file, query_no, unigram_lm_laplace_result[i][1], i+1,
                     unigram_lm_laplace_result[i][0])


def remove_previous_results():
    dir = os.path.dirname(__file__) + "/results"
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            os.remove(os.path.join(subdir, file))


if __name__ == '__main__':
    query_file = os.path.dirname(__file__) + '/AP_DATA/query_desc.51-100.short.txt'
    remove_previous_results()
    index = 'ap_dataset'
    doc_type = 'document'
    es = Elasticsearch(timeout=100)
    docs_info = {}
    avg_length = avg_doc_length(es, index, doc_type)
    voc_size = vocabulary_size(es, index)
    total_docs = es.count(index='ap_dataset')['count']
    with open(query_file) as f:
        lines = f.readlines()
        for line in lines:
            if not line.startswith("\n"):
                query_no, terms = analyze_query(es, index, line)
                print("query_no: " + query_no)
                compute_scores(es, index, query_no, terms)
