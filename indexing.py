from collections import defaultdict
from struct import *
from stemming.porter2 import stem
import tokenize

DEBUG = False
modes = ('naive', 'stopping', 'stemming', 'both')


def mem_stem(token, stem_mappings):
    if stem_mappings.has_key(token):
        return stem_mappings[token]
    else:
        stem_mappings[token] = stem(token)
        return stem_mappings[token]

# go through the whole corpus,
# compute ttf for each term
# compute doc_len for each doc
# map doc_number to integer id
# map term to integer id
# stopwords should not be removed in this process
def prepare(mode):
    assert mode in modes
    global ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex, doc_lens, stem_mappings
    term_id = doc_id = 1
    with open(tokenize.token_file, 'r') as f:
        for line in f:
            items = line.rstrip('\n').split(' ')
            doc_number = items[0]
            print 'prepare(): ', doc_number
            doc_tokens = items[1:]
            if len(doc_tokens) == 0:
                continue

            doc_ids[doc_number] = doc_id
            doc_lens[doc_id] = len(doc_tokens)
            doc_id += 1
            for token in doc_tokens:
                if mode in ['stemming', 'both']:
                    token = mem_stem(token, stem_mappings)

                ttf_counts[token] += 1
                if ttf_counts[token] == 1:
                    term_ids[token] = term_id
                    term_id += 1

    # write term info to file: (term, term_id)
    with open('index/{}/term_id_mappings.txt'.format(mode), "w") as f:
        for term in term_ids:
            f.write(term + " " + `term_ids[term]` + "\n")
    # write doc info to file: (doc_number, doc_id, doc_len)
    with open('index/{}/doc_id_mappings.txt'.format(mode), "w") as f:
        for doc_number in doc_ids:
            f.write(doc_number + " " + `doc_ids[doc_number]` + " " +
                    `doc_lens[doc_ids[doc_number]]` + "\n")


def stopping_filter(token_list, stopwords):
    return filter(lambda x: x not in stopwords, token_list)


# k means how many terms to index during each iteration of corpus
def indexing(mode, k):
    assert mode in modes
    global ttf_counts, corpus_dir, term_ids, doc_ids, tokenize_regex, stopwords, stem_mappings

    progress = 0
    cf = open('index/{}/category.txt'.format(mode), 'w')
    idxf = open('index/{}/index.txt'.format(mode), 'wb')
    while progress < len(ttf_counts):
        print 'progress:',
        index = defaultdict(lambda : {})
        # in stemming mode, those words are stemmed
        current_words = set(ttf_counts.keys()[progress : progress + k])
        print progress

        # go through the whole corpus
        tkf = open(tokenize.token_file, 'r')
        for line in tkf:
            items = line.rstrip('\n').split(' ')
            doc_number = items[0]
            if not doc_ids.has_key(doc_number): # this doc is skipped during prepare()
                continue
            doc_id = doc_ids[doc_number]
            # print 'indexing():', doc_number
            doc_tokens = items[1:]
            for idx, token in enumerate(doc_tokens):
                if mode in ['stopping', 'both'] and token in stopwords:
                    continue
                if mode in ['stemming', 'both']:
                    token = mem_stem(token, stem_mappings)
                if token not in current_words:
                    continue

                if not index[token].has_key(doc_id):
                    index[token][doc_id] = [idx]
                else:
                    index[token][doc_id].append(idx)
        tkf.close()

        # write to file
        for term in index:
            # print 'writing:',term
            begin_offset = idxf.tell()
            idxf.write(pack("=3L", term_ids[term], len(index[term]), ttf_counts[term]))
            for doc_id in index[term]:
                pos_arr = index[term][doc_id]
                idxf.write(pack("=LH", doc_id, len(pos_arr)))
                for pos in pos_arr:
                    idxf.write(pack("=H", pos))
            end_offset = idxf.tell()
            cf.write(" ".join([term, `begin_offset`, `end_offset`]) + '\n')

        progress += k
        if progress == k * 10:
            break
    idxf.close()
    cf.close()


def load_stopwords():
    s = set()
    with open('AP_DATA/stoplist.txt', 'r') as f:
        for line in f:
            if not line.startswith('\n'):
                line = line.rstrip('\n')
                s.add(line)
    return s


mode = 'naive'
ttf_counts = defaultdict(lambda : 0)
term_ids = {}
doc_ids = {}
doc_lens = {}
stopwords = load_stopwords()
stem_mappings = {}

def main():
    prepare(mode)
    print "term num: " + `len(term_ids)`
    print "doc num: " + `len(doc_ids)`
    indexing(mode, 1000)

if __name__ == '__main__':
    main()



