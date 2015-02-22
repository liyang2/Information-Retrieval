from collections import defaultdict
from struct import *
import load
import os
import re
import unittest

DEBUG = False

# go through the whole corpus,
# compute ttf, document length values for each distinct term
# map doc_number to integer id
# map term to integer id
def prepare():
    global ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex
    doc_lens = defaultdict(lambda : 0)
    term_id = 1
    doc_id = 1
    for doc_entry in load.parse_docs(corpus_dir, '', ''):
        doc_tokens = tokens(doc_entry['text'], tokenize_regex)

        if DEBUG:
            if doc_entry['_id'] == 'AP890622-0010':
                print "AP890622-0010:",
                print doc_tokens
                exit(0)

        if len(doc_tokens) == 0:
            continue
        doc_ids[doc_entry['_id']] = doc_id
        doc_id += 1
        doc_lens[doc_entry['_id']] = len(doc_tokens)
        for token in doc_tokens:
            ttf_counts[token] += 1
            if ttf_counts[token] == 1:
                term_ids[token] = term_id
                term_id += 1
    # write term info to file: (term, term_id)
    with open('index/term_id_mappings.txt', "w") as f:
        for term in term_ids.keys():
            f.write(term + " " + `term_ids[term]` + "\n")
    # write doc info to file: (doc_number, doc_id, token_number)
    with open('index/doc_id_mappings.txt', "w") as f:
        for doc_number in doc_ids.keys():
            f.write(doc_number + " " + `doc_ids[doc_number]` + " " +
                    `doc_lens[doc_number]` + "\n")

# return a list of tokens from text matching regex
def tokens(text, regex):
    return [token.lower() for token in re.findall(regex, text)]

def remove_front_underscore(str):
    while str.startswith('_'):
        str = str[1:]
    return str


# assume our memory is large enough
def indexing():
    global ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex
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
        f.write(pack("=3L", term_ids[term], len(index[term]), ttf_counts[term]))
        for doc_num in index[term]:
            pos_arr = index[term][doc_num]
            f.write(pack("=LH", doc_ids[doc_num], len(pos_arr)))
            for pos in pos_arr:
                f.write(pack("=H", pos))
    f.close()






if __name__ == '__main__':
    corpus_dir = os.path.dirname(__file__) + '/AP_DATA/ap89_collection'
    ttf_counts = defaultdict(lambda : 0)
    term_ids = {}
    doc_ids = {}
    # tokenize_regex = r"\w+(?:\.?\w+)*"
    tokenize_regex = r"[0-9A-Za-z]+\w*(?:\.?\w+)*"
    prepare()
    print "term num: " + `len(term_ids)`
    print "doc num: " + `len(doc_ids)`
    indexing()



