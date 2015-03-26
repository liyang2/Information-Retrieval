import requests
import time
from urlparse import urlparse


def get_wrapper(url, retries=1):
    while retries > 0:
        try:
            return requests.get(url, timeout=5)
        except:
            pass
        retries -= 1
    return None


def is_html(headers):
    if headers.get('content-type'):
        return 'text/html' in headers['content-type']
    else:
        return False


def domain(url):
    return urlparse(url).netloc


def whether_need_wait(url):
    if domain_visit.get(domain(url)):
        time_delta = time.time() - domain_visit[domain(url)]
        if time_delta < 1:
            return True
    return False


domain_visit = {}  # domain: last time visiting this domain
def fetch_html(url):
    if domain_visit.get(domain(url)):
        time_delta = time.time() - domain_visit[domain(url)]
        if time_delta < 1:
            print 'sleep:', 1 - time_delta
            time.sleep(1 - time_delta)
    domain_visit[domain(url)] = time.time()
    r = get_wrapper(url)
    if r and r.status_code == 200:
        return r.headers, unicode(r.content, r.encoding if r.encoding else 'utf-8', 'ignore') # so we are safe in unicode
    else:
        raise UrlException("Network error: %s" % url)


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
