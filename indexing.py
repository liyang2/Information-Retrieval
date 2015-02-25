from collections import defaultdict
from stemming.porter2 import stem
from tokenize import token_file
import os

DEBUG = False
modes = ('naive', 'stopping', 'stemming', 'both')


# build doc_info and term stemming mappings
def prepare():
    print 'start prepare()'
    doc_info = {} # doc_no: doc_id, doc_len
    line_info = {} # line_no: (beginOffset, endOffset) of token_file
    stem_mappings = {}
    doc_id = 1 # also acts as line #
    with open(token_file, 'r') as f:
        while True:
            # compute line_info
            beginOffset = f.tell()
            line = f.readline()
            if not line:
                break
            endOffset = f.tell()
            line_info[doc_id] = (beginOffset, endOffset)

            # compute doc_info
            items = line.rstrip('\n').split(' ')
            doc_number = items[0]
            doc_tokens = items[1:]
            doc_info[doc_number] = {'doc_id': doc_id, 'doc_len': len(doc_tokens)}
            doc_id += 1

            # compute stemming mappings
            for token in doc_tokens:
                if token in stem_mappings:
                    continue
                else:
                    stem_mappings[token] = stem(token)

    # write doc info to file: (doc_number, doc_id, doc_len)
    with open('index/doc_info.txt', "w") as f:
        for doc_number in doc_info:
            f.write(' '.join([doc_number, str(doc_info[doc_number]['doc_id']),
                              str(doc_info[doc_number]['doc_len'])]) + '\n')

    return line_info, doc_info, stem_mappings


def stopping_filter(token_list, stopwords):
    return filter(lambda x: x not in stopwords, token_list)

sequence = 0
def next_unused_number():
    global sequence
    sequence += 1
    return sequence

def get_lines(begin_line, end_line):
    global line_info
    lines = []
    with open(token_file, 'r') as f:
        beginOffset = line_info[begin_line][0]
        endOffset = line_info[end_line][1]
        f.seek(beginOffset)
        while f.tell() < endOffset:
            lines.append(f.readline())
    return lines


# k means how many terms to index during each iteration of corpus
def indexing(mode, k):
    global line_info
    cache, fname = divide_and_conquer(mode, 1, len(line_info), k)
    # write cache to file
    with open('index/{}/cache.txt'.format(mode), 'w') as f:
        for term in cache:
            f.write(' '.join([term, str(cache[term][0]), str(cache[term][1])])+'\n')


def divide_and_conquer(mode, begin_line, end_line, k):
    global doc_info
    cache = {} # term: (beginOffset, endOffset) in index file
    f_name = 'index/{}/index_{}.txt'.format(mode, next_unused_number())

    if end_line - begin_line < k: # proper size, we can manage
        index = {}
        lines = get_lines(begin_line, end_line)
        for line in lines:
            items = line.rstrip('\n').split(' ')
            doc_no, tokens = items[0], items[1:]
            doc_id = doc_info[doc_no]['doc_id']
            for pos, term in enumerate(tokens):
                if term in index:
                    index[term]['ttf'] += 1
                else:
                    index[term] = {'ttf': 1, 'df': 0, 'hits': {}}

                if doc_id in index[term]['hits']:
                    index[term]['hits'][doc_id].append(pos)
                else:
                    index[term]['df'] += 1
                    index[term]['hits'][doc_id] = [pos]
        # index has been built in memory
        with open(f_name, 'w') as f:
            for term in index:
                assert index[term]['df'] == len(index[term]['hits'])
                newline = ' '.join([term, str(index[term]['df']), str(index[term]['ttf'])])
                for doc_id in index[term]['hits']:
                    lst = [str(doc_id), str(len(index[term]['hits'][doc_id]))] + map(str, index[term]['hits'][doc_id])
                    newline += ' ' + ' '.join(lst)
                newline += '\n'
                cache[term] = write_file(f, newline)
        return cache, f_name
    else:
        mid_line = (begin_line + end_line) / 2
        cache1, fname1 = divide_and_conquer(mode, begin_line, mid_line, k)
        cache2, fname2 = divide_and_conquer(mode, mid_line+1, end_line, k)
        # merge two index file
        with open(f_name, 'w') as f:
            for term in cache1:
                if term in cache2: # combine the two
                    newline = combine_index(fname1, cache1[term], fname2, cache2[term])
                    cache[term] = write_file(f, newline)
                else:
                    line = read_file(fname1, cache1[term])
                    cache[term] = write_file(f, line)
            for term in cache2:
                if term not in cache1:
                    line = read_file(fname2, cache2[term])
                    cache[term] = write_file(f, line)
        os.remove(fname1)
        os.remove(fname2)
        return cache, f_name

def write_file(f, content):
    begin = f.tell()
    f.write(content)
    end = f.tell()
    return begin, end

def read_file(fname, offset_tuple):
    beginOffset, endOffset = offset_tuple
    with open(fname, 'r') as f:
        f.seek(beginOffset)
        content = f.read(endOffset - beginOffset)
    return content

def get_index_entry_parts(line):
    parts = line.rstrip('\n').split(' ')
    term, df, ttf, d_blocks = parts[0], parts[1], parts[2], parts[3:]
    return term, df, ttf, d_blocks # type: (string, string, string, list of string)

def combine_index(fname1, offset_tuple1, fname2, offset_tuple2):
    term1, df1, ttf1, d_blocks1 = get_index_entry_parts(read_file(fname1, offset_tuple1))
    term2, df2, ttf2, d_blocks2 = get_index_entry_parts(read_file(fname2, offset_tuple2))
    assert term1 == term2
    df = int(df1) + int(df2)
    ttf = int(ttf1) + int(ttf2)
    newline = ' '.join([term1, str(df), str(ttf)]) + ' ' + ' '.join(d_blocks1) + ' ' + ' '.join(d_blocks2)
    return newline + '\n'

def load_stopwords():
    s = set()
    with open('AP_DATA/stoplist.txt', 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            if line:
                s.add(line)
    return s

if __name__ == '__main__':
    mode = 'naive'
    stopwords = load_stopwords()
    line_info, doc_info, stem_mappings = prepare()
    print "unique terms: " + str(len(stem_mappings))
    print "docs: " + str(len(doc_info))
    indexing(mode, 1000)




