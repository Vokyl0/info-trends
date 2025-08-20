import requests
from bs4 import BeautifulSoup

def get_links(url):
    soup = BeautifulSoup(requests.get(url=url).text, "xml")
    links_xml = soup.find_all('link')
    links = []
    for link in links_xml:
        if 'articles' in link.text:
            links.append(link.text)
    return links

def get_article(url):
    soup = BeautifulSoup(requests.get(url=url).text, "xml")
    links_xml = soup.find('article').find_all('p')
    paragraphs = [p.text for p in links_xml]
    return ' '.join(paragraphs)
