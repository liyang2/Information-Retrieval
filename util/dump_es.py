
import elastic_helper
import elasticsearch

# dump content in Elastic search to a single file
# in order to merge with teammates

def dump_to_file(f, docno, title, head, text, raw):
    # f is the file object
    f.writelines('<DOC>\n')
    f.writelines('<DOCNO> ' + docno + ' </DOCNO>\n')
    if title is not None:
        f.writelines('<HEAD> ' + title + ' </HEAD>\n')
    if head is not None:
        # this is the html header
        f.writelines('<HTML-HEAD>\n' + head + '\n</HTML-HEAD>\n')

    text_to_write = '<TEXT> \n' + text + '</TEXT>\n'
    f.writelines(text_to_write.encode('utf-8', 'ignore'))

    raw_to_write = '<RAW> \n' + raw + '\n</RAW>\n'
    f.writelines(raw_to_write.encode('utf-8', 'ignore'))
    f.writelines('</DOC>\n')


def uuid_to_url(uuid):
    global uuid_url_mapping
    if uuid in uuid_url_mapping:
        return uuid_url_mapping[uuid]
    else:
        return None


def dump_link_graph(f, url, outlinks):
    outlinks = filter(lambda x: x, map(uuid_to_url, outlinks))
    f.writelines(url + ' ')
    f.writelines(' '.join(outlinks))
    f.writelines('\n')


def start_dump():
    global all_docs, uuid_url_mapping

    f = open('crawled.txt', 'w')
    f2 = open('link_graph.txt', 'w')

    print "Build mapping started"
    for item in all_docs:
        uuid_url_mapping[item['_id']] = item['fields']['url'][0]

    print "Build mapping finished"

    for item in all_docs:
        id = item['_id']
        url = item['fields']['url'][0]
        header = item['fields']['header'][0]
        # ins = item['fields']['in-links']  # in links are not dumped
        outs = item['fields']['out-links']
        dump_link_graph(f2, url, outs)

        print "dumping", url

        single = elastic_helper.get_single(id)
        text = single['_source']['text']
        html = single['_source']['html']

        dump_to_file(f, url, "", header, text, html)

    f.close()
    f2.close()


if __name__ == '__main__':
    all_docs = elastic_helper.get_all(12000)
    uuid_url_mapping = {}
    start_dump()
