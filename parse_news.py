import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

class Article:
    def __init__(self, url, title, description, text, pub_date):
        self.url = url
        self.title = title
        self.description = description
        self.text = text
        self.pub_date = pub_date

    def __str__(self):
        return (f'url: {self.url}\ntitle: {self.title}\ndescription: {self.description}' +
              f'\ntext: {self.text}\npub_date: {self.pub_date}\n')



def get_articles(url):
    soup = BeautifulSoup(requests.get(url=url).text, "xml")
    article_items = [item for item in soup.find_all('item') if 'articles' in item.find('link').text]
    articles = []

    if not os.path.exists('last_fetch_time.txt'):
        with open('last_fetch_time.txt', 'w') as f:
            f.write(datetime.min.strftime("%Y-%m-%d %H:%M:%S"))

    with open('last_fetch_time.txt', 'r') as last_fetch_file:
        last_fetch_time_str = last_fetch_file.read()

    last_fetch_time = datetime.strptime(last_fetch_time_str, "%Y-%m-%d %H:%M:%S") if last_fetch_time_str else datetime.min
    newest_pub_date = last_fetch_time

    articles_counter = 0

    for item in article_items:
        pub_date_str = item.find('pubDate').text
        pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")

        if pub_date > last_fetch_time:
            articles_counter += 1
            articles.append(get_article_from_item(item))
            if pub_date > newest_pub_date:
                newest_pub_date = pub_date

    with open('last_fetch_time.txt', 'w') as last_fetch_file:
        last_fetch_file.write(datetime.strftime(newest_pub_date, "%Y-%m-%d %H:%M:%S"))
    print(f'articles found: {articles_counter}')

    return articles


def get_article_from_item(item):
    pub_date_str = item.find('pubDate').text
    pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d %H:%M:%S")
    item_url = item.find('link').text
    title = item.find('title').text
    description = item.find('description').text
    text = get_article_text(item_url)
    article = Article(item_url, title, description, text, pub_date)
    return article


def get_article_text(url):
    soup = BeautifulSoup(requests.get(url=url).text, "xml")
    links_xml = soup.find('article').find_all('p')
    paragraphs = [p.text for p in links_xml]
    return ' '.join(paragraphs)




links_url = 'https://feeds.bbci.co.uk/news/world/rss.xml'
articles = get_articles(links_url)
with open('result_file', 'a', encoding='utf-8') as f:
    for article in articles:
        f.write(str(article) + '\n')

