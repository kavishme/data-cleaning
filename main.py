import psycopg2
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from html.entities import name2codepoint
from bs4 import BeautifulSoup

DB_NAME = 'buntu'
DB_ENDPOINT = 'localhost'
DB_USERNAME = 'postgres'
DB_PASSWORD = 'root'
DB_PORT = 5432  # default port
regex = re.compile('[%s]' % re.escape(string.punctuation))
wordnet = WordNetLemmatizer()


# Removes HTML or XML character references and entities from a text string.
#
# @param text The HTML (or XML) source text.
# @return The plain text, as a Unicode string, if necessary.
def unescape(text):
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return chr(int(text[3:-1], 16))
                else:
                    return chr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = chr(name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text  # leave as is
    return re.sub("&#?\w+;", fixup, text)


def cleanText(text):
    text = text.lower()
    soup = BeautifulSoup(unescape(text), "html.parser")
    text = soup.get_text()  # nltk.clean_html(unescape(text))

    tokens = word_tokenize(text)
    new_tokens = []
    for t in tokens:
        nt = regex.sub(u'', t)
        if not nt == u'' and nt not in stopwords.words('english'):
            new_tokens.append(wordnet.lemmatize(nt))

    return " ".join(new_tokens)


def main():
    result = ""
    try:
        conn = psycopg2.connect(host=DB_ENDPOINT, port=DB_PORT,
                                user=DB_USERNAME, password=DB_PASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        curins = conn.cursor()

        sql = """
            SELECT
            q.id               AS qid,
            q.tags             AS qtags,
            q.title            AS qtitle,
            q.body             AS qbody,
            q.acceptedanswerid AS acceptedans,
            a.id               AS aid,
            a.score            AS ascore,
            a.body             AS abody
            FROM posts q
            JOIN posts a
                ON a.parentid = q.id
            WHERE q.parentid IS NULL AND a.parentid IS NOT NULL
                AND a.score = (SELECT MAX(a1.score)
                                FROM posts q1
                                JOIN posts a1 ON a1.parentid = q1.id
                                WHERE q.id = q1.id
                                GROUP BY q1.id)
            ORDER BY q.id;
         """
        cur.execute(sql)
        result = cur.fetchone()
        while(result):
            result = list(result)
            for i in [2, 3, 7]:
                result[i] = cleanText(result[i])

            insert_sql = cur.mogrify("""INSERT INTO postscleaned (qid, qtags, qtitle, qbody, acceptedans, aid, ascore, abody)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s);""", result)
            curins.execute(insert_sql)
            conn.commit()
            print(result[0])
            result = cur.fetchone()
    except Exception as err:
        print("ERR: ")
        print(err)
        print(result)


if __name__ == "__main__":
    main()
