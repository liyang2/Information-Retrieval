
import elastic_helper
import elasticsearch

# dump content in Elastic search to a single file
# in order to merge with teammates

def dump_to_file(f, docno, title, head, text, raw):
    # address unicode issue
    docno = elastic_helper.unicode_to_bytes(docno)
    title = elastic_helper.unicode_to_bytes(title) if title else None
    text = elastic_helper.unicode_to_bytes(text)
    raw = elastic_helper.unicode_to_bytes(raw)

    f.writelines('<DOC>\n')
    f.writelines('<DOCNO> ' + docno + ' </DOCNO>\n')
    if title is not None:
        f.writelines('<HEAD> ' + title + ' </HEAD>\n')
    if head is not None:
        # this is the html header
        f.writelines('<HTML-HEAD>\n' + head + '\n</HTML-HEAD>\n')
    f.writelines('<TEXT> \n' + text + '</TEXT>\n')
    f.writelines('<RAW> \n' + raw + '\n</RAW>\n')
    f.writelines('</DOC>\n')


def dump_link_graph(f, url, outlinks):
    # address unicode issue
    url = elastic_helper.unicode_to_bytes(url)
    outlinks = map(elastic_helper.unicode_to_bytes, outlinks)

    f.writelines(url + ' ')
    f.writelines(' '.join(outlinks))
    f.writelines('\n')


def start_dump():
    all_docs = elastic_helper.get_all()

    f = open('crawled.txt', 'w')
    f2 = open('link_graph.txt', 'w')
    f3 = open('inlinks.txt', 'w')

    for item in all_docs:
        url = item['_id']
        header = item['_source']['header']
        ins = item['fields']['in-links']
        outs = item['_source']['out-links']
        text = item['_source']['text']
        html = item['_source']['html']

        print "dumping", url
        dump_link_graph(f2, url, outs)
        dump_link_graph(f3, url, ins)
        dump_to_file(f, url, None, header, text, html)
    f.close()
    f2.close()
    f3.close()

if __name__ == '__main__':
    start_dump()
