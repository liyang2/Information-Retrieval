
import elastic_helper

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
    f.writelines('<TEXT> \n' + text + '</TEXT>\n')
    f.writelines('<RAW> \n' + raw + '\n</RAW>\n')
    f.writelines('</DOC>\n')


def dump_link_graph(f, url, outlinks):
    f.writelines(url + ' ' + str(len(outlinks)) + ' ')
    f.writelines(' '.join(outlinks))
    f.writelines('\n')


def dump_to_disk():
    f = open('crawled.txt', 'w')
    f2 = open('link_graph.txt', 'w')
    for item in elastic_helper.get_all():
        print item
        id = item['_id']
        url = item['fields']['url']
        header = item['fields']['header']
        ins = item['fields']['in-links']
        outs = item['fields']['out-links']

        single = elastic_helper.get_single(id)
        text = single['_source']['text']
        html = single['_source']['html']

        dump_to_file()
    f.close()


if __name__ == '__main__':
    dump_to_disk()