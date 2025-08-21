import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator



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




def get_last_pub_date(dbname, user, host, port):
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(pub_date) FROM articles;")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result or datetime.min

def get_articles(url, last_pub_date):
    soup = BeautifulSoup(requests.get(url=url).text, "xml")
    article_items = [item for item in soup.find_all('item') if 'articles' in item.find('link').text]
    articles = []

    newest_pub_date = last_pub_date

    articles_counter = 0

    for item in article_items:
        pub_date_str = item.find('pubDate').text
        pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")

        if pub_date > last_pub_date:
            articles_counter += 1
            articles.append(get_article_from_item(item))
            if pub_date > newest_pub_date:
                newest_pub_date = pub_date

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


def create_articles_table(dbname, user, host, port):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port
    )
    cursor = conn.cursor()
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS articles (
        id SERIAL PRIMARY KEY,
        url TEXT NOT NULL,
        title TEXT,
        description TEXT,
        content TEXT,
        pub_date TIMESTAMP
    );
    '''

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print('Table articles created or already existed')


def process_articles(links_url, dbname, user, host, port):
    articles = get_articles(links_url, get_last_pub_date(dbname, user, host, port))
    insert_articles(dbname, user, host, port, articles)


def insert_articles(dbname, user, host, port, articles):
    conn = psycopg2.connect(
        dbname = dbname,
        user = user,
        host = host,
        port = port
    )
    cursor = conn.cursor()
    for article in articles:
        insert_article_query = '''
        INSERT INTO articles (url, title, description, content, pub_date) 
        VALUES (%s, %s, %s, %s, %s)
        '''
        cursor.execute(insert_article_query, (article.url, article.title, article.description, article.text, article.pub_date))
    conn.commit()
    print(f'{len(articles)} inserted')
    cursor.close()
    conn.close()


load_dotenv()
dbname = os.environ['DB_NAME']
user = os.environ['DB_USER']
host = os.environ['DB_HOST']
port = os.environ['DB_PORT']
links_url = 'https://feeds.bbci.co.uk/news/world/rss.xml'


daily_news_dag = DAG(
    dag_id='daily_news',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_articles_table,
    op_args=[dbname, user, host, port],
    dag=daily_news_dag
)

process_articles_task = PythonOperator(
    task_id='process_articles',
    python_callable=process_articles,
    op_args=[links_url, dbname, user, host, port],
    dag=daily_news_dag
)

create_table_task >> process_articles_task
