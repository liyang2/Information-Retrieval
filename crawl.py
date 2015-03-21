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
             'http://www.boxofficemojo.com/alltime/',
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
                                                and time_stayed > queue[ret]['count']):
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
            return item.get('href')
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
    return links


def start_crawl():
    crawled = {}  # url: set()
    queue = {}
    for url in seed_urls:
        queue[url] = {'in': set(), 'count': next(counter)}

    while queue and len(crawled) < MAX_CRAWL:
        url = next_best(queue)
        in_links_to_url = queue[url]['in']
        del queue[url]

        if not http.is_html(url):
            continue
        try:
            headers, html, clean, out = inspect(url)
            # discard its out links and refuse to store it to ES if page is not movie-related
            if not movie_related(clean):
                continue
        except http.UrlException:
            continue

        print "current url", url, "in-link: ", len(in_links_to_url)
        print "current progress %d / %d" % (len(crawled), MAX_CRAWL)
        crawled[url] = in_links_to_url
        elastic_helper.save_to_es(url, clean, html, str(headers), [], out)  # missing in-links
        for out_link in out:
            if out_link in crawled:
                crawled[out_link].add(url)
            elif out_link in queue:
                queue[out_link]['in'].add(url)
            else:
                queue[out_link] = {'in': set(), 'count': next(counter)}

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


