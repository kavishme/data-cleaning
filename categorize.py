import psycopg2
import re
import os
import datetime
import csv

DB_NAME = 'buntu'
DB_ENDPOINT = 'localhost'
DB_USERNAME = 'postgres'
DB_PASSWORD = 'root'
DB_PORT = 5432  # default port

regex = re.compile('<([\w\.\-\_\s]+)>')

"""
Returns list of tags parsed from tag string.
"""
def getTags(text):
    return regex.findall(text.lower())

"""
Return dictionary of tags - posts
"""
def getPostsByTags():
    try:
        postsByTags = {}
        conn = psycopg2.connect(host=DB_ENDPOINT, port=DB_PORT,
                                user=DB_USERNAME, password=DB_PASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        curins = conn.cursor()

        sql = """
            SELECT *
            FROM postscleaned
            ORDER BY qid;
         """
        cur.execute(sql)
        result = cur.fetchone()
        while(result):
            for tag in getTags(result[1]):
                if tag not in postsByTags:
                    postsByTags[tag] = []
                postsByTags[tag].append(result)

            print(result[0])
            result = cur.fetchone()
        return postsByTags
    except Exception as err:
        print("ERR: ")
        print(err)


"""
Store posts to CSV files by tags
"""
def toCSV(postBytags):
    try:
        outdir = "output_" + datetime.datetime.now().strftime('%Y%d%m%H%M')
        os.makedirs(outdir)
        for tag in postBytags:
            with open(os.path.join(outdir, tag+".csv"), 'w') as of:
                wr = csv.writer(of, quoting=csv.QUOTE_ALL)
                wr.writerows(postBytags[tag])
    except Exception as err:
        print("ERR: ")
        print(err)

if __name__ == "__main__":
    p = getPostsByTags()
    toCSV(p)
