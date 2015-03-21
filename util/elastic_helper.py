# encoding=utf-8
from elasticsearch import Elasticsearch
import uuid

index_name = 'hw3'
doc_type = 'page'


def init_index():
    client = Elasticsearch(timeout=100)

    if client.indices.exists(index_name):
        client.indices.delete(index_name)

    # create empty index
    client.indices.create(
        index=index_name,
        body={
            'settings': {
                'index': {
                    'store': {
                        'type': 'default'
                    },
                    'number_of_shards': 1,
                    'number_of_replicas': 0
                },
                "analysis": {
                    "analyzer": {
                        "my_analyzer": {
                            "type": "custom",
                            "tokenizer": "myTokenizer",
                            "filter": ["myLengthFilter", "myStopFilter"]
                        }
                    },
                    "tokenizer": {
                        "myTokenizer": {
                            "type": "standard",
                            "max_token_length": 255
                        }
                    },
                    "filter": {
                        "myLengthFilter": {
                            "type": "length",
                            "max": 255
                        },
                        "myStopFilter":{
                            "type": "stop",
                            "stopwords_path": "stoplist.txt"
                        }
                    }
                }
            }
        }
    )

    client.indices.put_mapping(
        index=index_name,
        doc_type=doc_type,
        body={
            doc_type: {
                "properties": {
                    "url":{ # canonical url
                        "type": "string",
                        "store": "true",
                    },
                    "html": { # raw html text
                        "type": "string",
                        "store": 'true',
                    },
                    "header": { # http header
                        "type": "string",
                        "store": "true",
                    },
                    "text": { # cleaned main content text of html
                        "type": "string",
                        "store": 'true',
                        "index": "analyzed",
                        "analyzer": "my_analyzer"
                    },
                    # in-links and out-links are array type
                    # but in elastic search, there is no special mapping for array type
                    "in-links": {
                        "type": "string",
                        "store": "true",
                    },
                    "out-links": {
                        "type": "string",
                        "store": "true"
                    }
                }
            }
        }
    )


def url_to_uuid(url):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url.encode('utf-8')))



# assume in_link_list and out_link_list are lists of urls, not ids
def save_to_es(url, clean_text, raw_html, http_header, in_link_list, out_link_list):
    es = Elasticsearch()
    in_link_list = map(url_to_uuid, in_link_list)
    out_link_list = map(url_to_uuid, out_link_list)
    doc = {
        'url': url,
        'html': raw_html,
        'header': http_header,
        'text': clean_text,
        'in-links': in_link_list,
        'out-links': out_link_list
    }
    # try:
    ret = es.index(index=index_name,
               doc_type=doc_type,
               id=url_to_uuid(url),
               body=doc)
    # except:
    #     raise Exception('UnicodeDecodeError, url is', url)

    return ret['created']


def es_update_inlinks(url, in_links):
    es = Elasticsearch()
    in_links = map(url_to_uuid, in_links)
    doc = {
        'doc': {
            'in-links': in_links,
            'in-links-count': len(in_links)
        }
    }
    ret = es.update(index=index_name,
                   doc_type=doc_type,
                   id=url_to_uuid(url),
                   body=doc)


if __name__ == '__main__':
    str1 = u'abc我'
    print uuid.uuid5(uuid.NAMESPACE_DNS, str1.encode('utf-8'))
    print uuid.uuid5(uuid.NAMESPACE_DNS, 'abc')