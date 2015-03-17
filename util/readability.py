import urllib
import urllib2
import urlparse
import json
from bs4 import BeautifulSoup

def get_clean_content(url):
    api_token = '# use your own token #'
    with open('secret.txt') as f:
        api_token = f.readline()

    r = urlparse.ParseResult(scheme='http',
                             netloc='www.readability.com',
                             path='/api/content/v1/parser',
                             params='',
                             query=urllib.urlencode({'url': url, 'token': api_token}),
                             fragment='')
    response = urllib2.urlopen(r.geturl()).read() # in json format
    soup = BeautifulSoup(json.loads(response)['content'])
    return soup.get_text()

if __name__ == '__main__':
    my_url = 'http://blog.readability.com/2011/02/step-up-be-heard-readability-ideas'
    print get_clean_content(my_url)