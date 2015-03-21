import requests
import time
from urlparse import urlparse


def is_html(url):
    r = requests.head(url)
    if r.headers.get('content-type'):
        return 'text/html' in r.headers['content-type']
    else:
        return False


def domain(url):
    return urlparse(url).netloc


domain_visit = {}  # domain: last time visiting this domain
def fetch_html(url):
    if domain_visit.get(domain(url)):
        time_delta = time.time() - domain_visit[domain(url)]
        if time_delta < 1:
            time.sleep(1 - time_delta)
    domain_visit[domain(url)] = time.time()
    r = requests.get(url)
    if r.status_code == 200:
        return r.headers, unicode(r.content, r.encoding) # so we are safe in unicode
    else:
        raise UrlException("Status code != 200")


class UrlException(Exception):
    pass


if __name__ == '__main__':
    headers, content = fetch_html('http://www.boxofficemojo.com/news/?ref=ft')
    # content.encode('ISO-8859-1')
    print type(content)
    # content = unicode(content, 'ISO-8859-1')
    content = content.decode('ISO-8859-1')

    print type(content)
    print repr(content)
