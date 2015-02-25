import load
import re

# process the whole collection of docs
# keep only DOCNO and TEXT fields and tokenize TEXT string

tokenize_regex = r"[0-9A-Za-z]+\w*(?:\.?\w+)*"
token_file = 'index/ap00all.txt'
corpus_dir = 'AP_DATA/ap89_collection'

# return a list of tokens from text matching regex
def tokens(text, regex):
    return [token.lower() for token in re.findall(regex, text)]

if __name__ == '__main__':
    with open(token_file, 'w') as f:
        for doc_entry in load.parse_docs(corpus_dir, '', ''):
            doc_number = doc_entry['_id']
            doc_tokens = tokens(doc_entry['text'], tokenize_regex)
            if len(doc_tokens) == 0: # filter out empty documents
                continue
            f.write(doc_number + " " + ' '.join(doc_tokens) + '\n')
