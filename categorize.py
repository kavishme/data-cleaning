import psycopg2
import re
import os
import datetime
import csv
import random
import copy

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
            SELECT qtags, qtitle, qbody, abody
            FROM postscleaned;
         """
        cur.execute(sql)
        result = cur.fetchone()
        while(result):
            for tag in getTags(result[0]):
                if tag not in postsByTags:
                    postsByTags[tag] = []
                postsByTags[tag].append(list(result))

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
        return outdir
    except Exception as err:
        print("ERR: ")
        print(err)


def random_answer(posts):
    cats = list(posts.keys())
    outposts = {}
    for c in cats:
        outposts[c] = []
        for rec in posts[c]:
            
            rc = random.choice(cats)
            ra = random.choice(posts[rc])
            
            wrec = copy.deepcopy(rec)
            wrec[-1] = ra[-1]
            
            rec.append(1)
            outposts[c].append(tuple(rec))
            wrec.append(0)
            outposts[c].append(tuple(wrec))
    return outposts

def cat_count(posts, dir):
    filepath = os.path.join(dir,"cat_count.csv")
    f = open(filepath, 'w')
    t = []
    for p in posts:
        t.append( (p,len(posts[p])) )
    t = sorted(t, key=lambda lts: lts[1])
    wr = csv.writer(f, quoting=csv.QUOTE_ALL)
    wr.writerows(t)
    f.close()

if __name__ == "__main__":
    p = getPostsByTags()
    #p=random_answer(p)
    dir = toCSV(p)
    cat_count(p, dir)

