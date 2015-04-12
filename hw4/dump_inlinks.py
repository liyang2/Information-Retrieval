
# dump a link graph file from ES with format:
# url [inlink1] [inlink2] ...

from util import elastic_helper
from collections import defaultdict

output_file = 'inlinks.txt'

def dump_inlinks():
    inlinks = defaultdict(list)  # url: [] (inlinks list)
    inlinks2 = defaultdict(list)  # url: [] (inlinks list)
    uuid_hash = {}
    for item in elastic_helper.get_all():
        try:
            id = item["_id"]
            print id
            url = item['_source']['url']
            ins = item['_source']['in-links']
            uuid_hash[id] = url
            for inlink in ins:
                inlinks[url].append(inlink)
        except KeyError:
            continue

    print "half done"

    for url, uuid_lst in inlinks.iteritems():
        for uuid in uuid_lst:
            if uuid in uuid_hash:
                inlinks2[url].append(uuid_hash[uuid])

    # start dumping to file
    with open(output_file, 'w') as f:
        for url, inlink_list in inlinks2.iteritems():
            inlink_list = map(lambda x: x.encode('utf-8', errors='ignore'), inlink_list)
            f.write(url.encode('utf-8', errors='ignore') + " " + " ".join(inlink_list) + "\n")


if __name__ == '__main__':
    dump_inlinks()