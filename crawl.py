from collections import defaultdict
import itertools
import time
from bs4 import BeautifulSoup
import urllib
import re

from util import elastic_helper
from util import urlnorm
from util import http


MAX_CRAWL = 12000
seed_urls = ['http://en.wikipedia.org/wiki/List_of_highest-grossing_films',
             'http://www.the-numbers.com/movie/records/',
             'http://www.filmsite.org/boxoffice.html',
             'http://www.imdb.com/boxoffice/alltimegross',
             'http://fortune.com/2015/02/23/10-animated-blockbusters/',
             'http://en.wikipedia.org/wiki/List_of_Batman_films_cast_members',
             'http://en.wikipedia.org/wiki/The_Dark_Knight_(film)']

counter = itertools.count()


def next_best(queue):
    if not queue:
        raise KeyError('Queue is empty')

    ret = queue.iterkeys().next()
    for url in queue:
        if url in seed_urls:
            return url
        in_links = len(queue[url]['in'])
        time_stayed = queue[url]['count']
        if in_links > len(queue[ret]['in']) or (in_links == len(queue[ret]['in'])
                                                and time_stayed < queue[ret]['count']):
            ret = url
    return ret


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', unicode(element)):
        return False
    return True

# text = soup.findAll(text=True)
def clean_text(texts):
    visible_text = filter(visible, texts)
    return ''.join(visible_text)

def remove_html_tags(html):
    soup = BeautifulSoup(html)
    return clean_text(soup.findAll(text=True))


def movie_related(page_text):
    page_text = page_text.lower()
    keywords = ['movie', 'film']
    for kw in keywords:
        if kw in page_text:
            return True
    return False


# return (http header, raw_html, clean_text, out_links)
def inspect(url):
    headers, html = http.fetch_html(url)
    clean = remove_html_tags(html)
    out = out_links(html, url)
    return headers, html, clean, out


def out_links(html, url_str):
    def my_map_func(item):
        if item is None:
            return None
        try:
            return unicode(item.get('href'))
        except:
            print "This url seems to have unicode", item.get(u'href')
            return None

    def my_filter(item):
        if item is None or item.startswith('#'):
            return False
        return True

    links = set()

    soup = BeautifulSoup(html)
    original_links = filter(my_filter, map(my_map_func, soup.find_all('a')))

    for link in original_links:
        if link.startswith('//'):
            links.add('http://'+link[2:])
        elif link.startswith('/'):
            links.add('http://' + http.domain(url_str)+link)
        elif link.startswith('http'):
            links.add(link)
        else:  # like hello.html
            links.add(url_str+'/'+link)
    links = set(map(urlnorm.norms, links))
    if None in links:
        links.remove(None)
    return links


def start_crawl():
    saved = 0  # number of pages saved to ES
    stop_crawl = False
    crawled = {}  # url: set()
    queue = {}
    rubbish = set()
    for url in seed_urls:
        headers, html = http.fetch_html(url)
        queue[url] = {'in': set(), 'count': next(counter), 'headers': headers, 'html': html}

    # we assume all elements in queue has already been crawled but not analyzed
    while queue and len(crawled) < MAX_CRAWL:
        url = next_best(queue)

        in_links_to_url, c, headers, html = \
            queue[url]['in'], queue[url]['count'], queue[url]['headers'], queue[url]['html']
        clean_html = remove_html_tags(html)
        out = out_links(html, url)

        del queue[url]
        print "off queue:", url

        crawled[url] = in_links_to_url
        elastic_helper.save_to_es(url, clean_html, html, str(headers), [], out)  # missing in-links
        saved += 1

        if saved + len(queue) > MAX_CRAWL:
            stop_crawl = True
        if stop_crawl:
            continue

        for out_link in out:
            if out_link in rubbish:
                continue
            if out_link in crawled:
                crawled[out_link].add(url)
            elif out_link in queue:
                queue[out_link]['in'].add(url)
            else:
                # crawl then decide whether to enqueue
                if not http.is_html(out_link):
                    rubbish.add(out_link)
                    continue
                headers_out, html_out = http.fetch_html(out_link)
                if not movie_related(html_out):
                    rubbish.add(out_link)
                    continue

                queue[out_link] = {'in': set([url]), 'count': next(counter), 'headers': headers_out, 'html': html_out}
                print "saved + len(queue) : %d" % (saved + len(queue))
                print "enqueue:", out_link

    # update in-links to ES
    for url in crawled:
        print 'Updating in-links for', url
        elastic_helper.es_update_inlinks(url, crawled[url])

    # print problem urls
    for p in urlnorm.problem_url_set:
        print p


if __name__ == '__main__':
    elastic_helper.init_index()
    start_time = time.time()
    start_crawl()
    end_time = time.time()
    print "Time elapsed: %d mins %d secs" % ((end_time - start_time) / 60, (end_time-start_time) % 60)


# if __name__ == '__main__':
#     inspect('http://en.wikipedia.org/wiki/Metahuman')
