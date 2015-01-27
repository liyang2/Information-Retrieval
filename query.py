from __future__ import division
from __future__ import print_function
import os
import json
import re
from elasticsearch import Elasticsearch
import heapq
from collections import defaultdict

def avg_doc_length(es, index, doc_type):
    return es.search(index=index,doc_type=doc_type,body={
        "aggs" : {
            "avg_length" : {
                "avg" : {
                    "script" : "_source.text.length()"
                }
            }
        }
    })['aggregations']['avg_length']['value']

def tf(jsonObj):
    str = json.dumps(jsonObj)
    return float(re.search(r"(?<=tf\(freq=)\d+\.\d+(?=\))", str).group(0))

# return rows of information of docs which contains term
def fetch_postings(es, index, term):
    print("term: " + term)
    total_docs = es.count(index='ap_dataset')['count']
    results = es.search(
        index=index,
        doc_type='document',
        size=total_docs,
        explain=True, # so that we have tf, df info
        body={
            "query" : {
                "match" : {
                    "text" : term
                }
            }
        }
    )
    for hit in results['hits']['hits']:
        yield hit['_id'], tf(hit['_explanation']), \
              len(hit['_source']['text']),
        # id, tf, doc_length,

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


def okapi_tf(es, index, query_no, query_terms, output_path):
    global avg_length
    scores = defaultdict(lambda: 0.0)
    for q_term in query_terms:
        for id, tf, doc_len in fetch_postings(es, index, q_term):
            scores[id] += tf / (tf + 0.5 + 1.5 * (doc_len/avg_length))
    result_list = get_top_k_docs(scores, 100)
    for i, value in enumerate(result_list):
        write_result(output_path, query_no, value[1], i+1, value[0])

def remove_previous_results():
    dir = os.path.dirname(__file__) + "/results"
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            os.remove(os.path.join(subdir, file))

if __name__ == '__main__':
    query_file = os.path.dirname(__file__) + '/AP_DATA/query_desc.51-100.short.txt'
    okapi_tf_file = os.path.dirname(__file__) + '/results/okapi_tf.txt'
    remove_previous_results()
    index = 'ap_dataset'
    doc_type = 'document'
    es = Elasticsearch(timeout=100)
    avg_length = avg_doc_length(es, index, doc_type)
    with open(query_file) as f:
        lines = f.readlines()
        for line in lines:
            if not line.startswith("\n"):
                query_no, terms = analyze_query(es,index, line)
                print("query_no: " + query_no)
                okapi_tf(es, index, query_no, terms, okapi_tf_file)


