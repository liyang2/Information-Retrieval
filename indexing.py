from collections import defaultdict
from struct import *
import load
import os
import re

# go through the whole corpus,
# compute df and ttf values for each distinct term
# map doc_number to integer id
# map term to integer id
def prepare_df_ttf():
    global df_counts, ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex
    term_id = 1
    doc_id = 1
    for doc_entry in load.parse_docs(corpus_dir, '', ''):
        doc_ids[doc_entry['_id']] = doc_id
        doc_id += 1
        distinct_tokens = set()
        for token in tokens(doc_entry['text'], tokenize_regex):
            distinct_tokens.add(token)
            ttf_counts[token] += 1
            if ttf_counts[token] == 1:
                term_ids[token] = term_id
                term_id += 1
        for token in distinct_tokens:
            df_counts[token] += 1
    # write term -> id mappings to file


# return a list of tokens from text matching regex
def tokens(text, regex):
    return re.findall(regex, text)

def pack_array(format, array):
    str = ""
    for ele in array:
        str += pack(format, ele)
    return str

# assume our memory is large enough
def indexing():
    global df_counts, ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex
    index = {}
    for term in term_ids.keys():
        index[term] = {}
    for doc_entry in load.parse_docs(corpus_dir, '', ''):
        doc_number = doc_entry['_id']
        for idx, token in enumerate(tokens(doc_entry['text'], tokenize_regex)):
            if not index[token].has_key(doc_number):
                index[token][doc_number] = [idx]
            else:
                index[token][doc_number].append(idx)
    # write to file
    f = open('index/index.txt', 'wb')
    for term in index.keys():
        print term
        # f.write(`term_ids[term]` + " " + `df_counts[term]` + " " + `ttf_counts[term]`)
        f.write(pack("=3L", term_ids[term], df_counts[term], ttf_counts[term]))
        for doc_num in index[term]:
            pos_arr = index[term][doc_num]
            # f.write(" " + `doc_ids[doc_num]` + " " + `len(pos_arr)`)
            f.write(pack("=LH", doc_ids[doc_num], len(pos_arr)))
            f.write(pack_array("=H", pos_arr))
        f.write("\n")
    f.close()






if __name__ == '__main__':
    corpus_dir = os.path.dirname(__file__) + '/AP_DATA/ap89_collection'
    df_counts = defaultdict(lambda : 0)
    ttf_counts = defaultdict(lambda : 0)
    term_ids = {}
    doc_ids = {}
    tokenize_regex = r"\w+(?:\.?\w+)*"
    prepare_df_ttf()
    print "term num: " + `len(term_ids)`
    print "doc num: " + `len(doc_ids)`
    indexing()


