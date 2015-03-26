from collections import defaultdict
import itertools
import time
from bs4 import BeautifulSoup
import urllib
import re
from pqdict import PQDict

from util import elastic_helper
from util import urlnorm
from util import http


MAX_CRAWL = 12000
seed_urls = ['http://en.wikipedia.org/wiki/List_of_highest-grossing_films',
             'http://www.boxofficemojo.com/',
             'http://www.the-numbers.com/movie/records/',
             'http://www.filmsite.org/boxoffice.html',
             'http://www.imdb.com/boxoffice/alltimegross',
             'http://fortune.com/2015/02/23/10-animated-blockbusters/',
             'http://en.wikipedia.org/wiki/List_of_Batman_films_cast_members',
             'http://en.wikipedia.org/wiki/The_Dark_Knight_(film)']

counter = itertools.count()


def next_best(queue, last):
    if not queue:
        raise KeyError('Queue is empty')

    if last and not http.whether_need_wait(last):
        last = None

    queue_iter = queue.iterkeys()
    ret = queue_iter.next()
    while last and http.domain(last) == http.domain(ret):
        ret = queue_iter.next()

    for url in queue:
        if url in seed_urls:
            return url
        if last and http.domain(url) == http.domain(last):
            continue
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
    crawled = {}  # url: set() of in-links
    pq = PQDict.maxpq()
    rubbish = set()  # urls that are not movie-related

    for url in seed_urls:
        pq[url] = (0, next(counter) * -1, set())  # (len(in_links), time stayed in queue, in_links)

    while pq and len(crawled) < MAX_CRAWL:
        url, value = pq.popitem()
        temp_save = []
        while http.whether_need_wait(url):
            temp_save.append((url, value))
            url, value = pq.popitem()
        for url_t, value_t in temp_save:
            pq[url_t] = value_t

        in_links_to_url = value[2]

        try:
            headers, html, clean, out = inspect(url)

            # discard if page is not html type
            if not http.is_html(headers):
                rubbish.add(url)
                print 'skipping (not html)'
                continue

            # discard its out links and refuse to store it to ES if page is not movie-related
            if not movie_related(clean):
                rubbish.add(url)
                print 'skipping (not movie)'
                continue
        except http.UrlException:
            print 'skipping (UrlException)'
            continue

        print "current(%d)" % len(crawled), url, "in-link: ", len(in_links_to_url)
        crawled[url] = in_links_to_url
        elastic_helper.save_to_es(url, clean, html, str(headers), [], out)  # missing in-links
        for out_link in out:
            if out_link in rubbish:
                continue
            if out_link in crawled:
                crawled[out_link].add(url)
            elif out_link in pq:
                item = pq[out_link]
                item[2].add(url)
                pq.updateitem(out_link, (item[0]+1, item[1], item[2]))
            else:
                pq[out_link] = (1, next(counter) * -1, set([url]))

    # update in-links to ES
    for url in crawled:
        print 'Updating in-links for', url
        elastic_helper.es_update_inlinks(url, crawled[url])



if __name__ == '__main__':
    elastic_helper.init_index()
    start_time = time.time()
    start_crawl()
    end_time = time.time()
    print "Time elapsed: %d mins %d secs" % ((end_time - start_time) / 60, (end_time-start_time) % 60)

