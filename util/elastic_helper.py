# encoding=utf-8
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import uuid

index_name = 'hw3'
doc_type = 'page'

uuid_to_url = {}
def url_to_uuid(url):
    uuid_str = str(uuid.uuid5(uuid.NAMESPACE_URL, url.encode('utf-8')))
    uuid_to_url[uuid_str] = url
    return uuid_str


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
                        "analyzer": "standard"
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


def unicode_to_bytes(str):
    return str.encode('utf-8', 'ignore')


# assume in_link_list and out_link_list are lists of urls, not ids
def save_to_es(url, clean_text, raw_html, http_header, in_link_list, out_link_list):
    # unicode to bytes
    url = unicode_to_bytes(url)
    in_link_list = map(unicode_to_bytes, in_link_list)
    out_link_list = map(unicode_to_bytes, out_link_list)

    es = Elasticsearch()
    doc = {
        'url': url,
        'html': raw_html,
        'header': http_header,
        'text': clean_text,
        'in-links': in_link_list,
        'out-links': out_link_list
    }
    ret = es.index(index=index_name,
               doc_type=doc_type,
               id=url,
               body=doc)
    return ret['created']


def es_update_inlinks(url, in_links):
    url = unicode_to_bytes(url)
    in_links = map(unicode_to_bytes, in_links)

    es = Elasticsearch()
    doc = {
        'doc': {
            'in-links': in_links,
            'in-links-count': len(in_links)
        }
    }
    ret = es.update(index=index_name,
                   doc_type=doc_type,
                   id=url,
                   body=doc)


# returned fields: url, header, in-links, out-links
# 'text' and 'html' are skipped because too big
def get_all():
    es = Elasticsearch(timeout=100)

    ret = helpers.scan(es,
                       index=index_name,
                       doc_type=doc_type,
                       scroll='5m')
    return ret


def get_single(doc_id):
    es = Elasticsearch(timeout=100)
    ret = es.get(index=index_name, doc_type=doc_type, id=doc_id)
    return ret


if __name__ == '__main__':
    x = get_all()
    print next(x)