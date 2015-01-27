from elasticsearch import Elasticsearch
import os
from elasticsearch.helpers import streaming_bulk

def create_ap_index(client, index, doc_type):
    if client.indices.exists(index):
        client.indices.delete(index)

    # create empty index
    client.indices.create(
        index=index,
        body={
            'settings': {
                'index': {
                    'store': {
                        'type': 'default'
                    },
                    'number_of_shards': 1,
                    'number_of_replicas': 0
                },
                'analysis': {
                    'analyzer': {
                        'my_english': {
                            'type': 'english',
                            'stopwords_path': 'stoplist.txt'
                        }
                    }
                }
            }
        }
    )

    client.indices.put_mapping(
        index=index,
        doc_type=doc_type,
        body={
            "document": {
                "properties": {
                    "docno": {
                        "type": "string",
                        "store": 'true',
                        "index": "not_analyzed"
                    },
                    "text": {
                        "type": "string",
                        "store": 'true',
                        "index": "analyzed",
                        "term_vector": "with_positions_offsets_payloads",
                        "analyzer": "my_english"
                    }
                }
            }
        }
    )

def parse_docs(dir, index, doc_type):
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            with open(os.path.join(subdir, file)) as f:
                line = f.readline()
                while line:
                    if line.startswith("<DOCNO>"):
                        docno = line[7:-9].strip()
                        text = ""
                    elif line.startswith("<TEXT>"):
                        line = f.readline()
                        while not line.startswith("</TEXT>"):
                            text += line
                            line = f.readline()
                    elif line.startswith("</DOC>"):
                        yield {
                            '_index': index,
                            '_type': doc_type,
                            '_id': docno,
                            'text': text
                        }
                    line = f.readline()

def load_docs(client, dir, index, doc_type):
    for ok, result in streaming_bulk(
        client,
        parse_docs(dir, index, doc_type)
    ):
        action, result = result.popitem()
        doc_id = result['_id']
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            print(doc_id)


if __name__ == '__main__':
    dir = os.path.dirname(__file__) + '/AP_DATA/ap89_collection'
    index = 'ap_dataset'
    doc_type = 'document'
    es = Elasticsearch(timeout=100)
    create_ap_index(es, index, doc_type)
    load_docs(es, dir, index, doc_type)

