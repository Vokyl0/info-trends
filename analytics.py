import psycopg2
import os
from nltk import bigrams
from dotenv import load_dotenv
import spacy
from collections import Counter
from datetime import datetime

nlp = spacy.load("en_core_web_sm")

def preprocess_article(text: str) -> list[str]:
    doc = nlp(text)
    tokens = list({token.lemma_.lower() for token in doc
                   if token.pos_ in ("NOUN", "PROPN") and not token.is_stop and not token.is_punct})
    return tokens

def get_top_words(text: str, n: int = 50):
    word_freq = Counter(text)
    return word_freq.most_common(n)


def get_top_bigrams(text: str, n: int = 50):
    word_freq = Counter(text)
    return word_freq.most_common(n)

load_dotenv()

def get_all_tokens(start_date=datetime(2025, 1, 1), end_date=None):
    if end_date is None:
        end_date = datetime.now()
    dbname = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    host = os.environ['DB_HOST']
    port = os.environ['DB_PORT']

    conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            port=port
        )
    cursor = conn.cursor()

    query = '''
    SELECT title -- can also be 'description' or 'content'
    FROM articles
    WHERE pub_date >= %s AND pub_date < %s
    '''

    cursor.execute(query, (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')))
    articles = cursor.fetchall()
    tokens_lists = [preprocess_article(article[0]) for article in articles]
    all_tokens = []

    for sublist in tokens_lists:
        for item in sublist:
            all_tokens.append(item)

    cursor.close()
    conn.close()
    return all_tokens

text = get_all_tokens()
text_bigrams = list(bigrams(text))
top_words = get_top_words(text, 50)
top_bigrams = get_top_bigrams(text_bigrams, 10)

print('Top words:', top_words)
print('Top bigrams:', top_bigrams)
