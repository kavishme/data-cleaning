import psycopg2
import re
import os
import datetime
import csv
import random
import copy
import json

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
def toCSV(postBytags, singleFile=False, filename="all_data.csv"):
    try:
        outdir = "output_" + datetime.datetime.now().strftime('%Y%d%m%H%M')
        os.makedirs(outdir)
        header=["qtags", "qtitle", "qbody","abody","label"]
        if not singleFile:
            for tag in postBytags:
                with open(os.path.join(outdir, tag+".csv"), 'w') as of:
                    wr = csv.writer(of, quoting=csv.QUOTE_ALL)
                    wr.writerow(header)
                    wr.writerows(postBytags[tag])
        else:
            with open(os.path.join(outdir, filename), 'w') as of:
                wr = csv.writer(of, quoting=csv.QUOTE_ALL)
                wr.writerow(header)
                for tag in postBytags:
                    wr.writerows(postBytags[tag])
        return outdir
    except Exception as err:
        print("ERR: ")
        print(err)


def randomizeAnswerAndLabel(posts):
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


def filterbySize(posts, minl, maxl, filterIndex):
    cats = list(posts.keys())
    outposts = {}
    for c in cats:
        outposts[c] = []
        for rec in posts[c]:
            data = str(rec[filterIndex])
            # print(data)
            data = data.split()
            if len(data)>= minl and len(data)<=maxl:
                outposts[c].append(rec)
    return outposts

def wordToVec(posts):
    cats = list(posts.keys())
    bagOfWords = {}
    for c in cats:
        for rec in posts[c]:
            # 0:"qtags", 1:"qtitle", 2:"qbody", 3:"abody", 4:"label"
            data = []
            for i in [1,2,3]:
                data.extend(str(rec[i]).split())

            for word in data:
                bagOfWords[word] = 0
    
    i = 0
    for item in bagOfWords:
        bagOfWords[item] = i
        i += 1
    
    outposts = {}
    for c in cats:
        outposts[c] = []
        for rec in posts[c]:
            vecRec = []
            vecRec.append(rec[0])
            
            for i in [1,2,3]:
                vecRec.append(' '.join( [str(bagOfWords[word]) for word in str(rec[i]).split()] ))
            
            vecRec.extend(rec[4:])
            outposts[c].append(tuple(vecRec))

    return outposts, bagOfWords

if __name__ == "__main__":
    p = getPostsByTags()
    
    # adds label 0 and 1
    p = randomizeAnswerAndLabel(p)

    # filterIndex 0:"qtags", 1:"qtitle", 2:"qbody", 3:"abody", 4:"label"
    # filter all records for abody of min length 15 and max length of 90
    p = filterbySize(p, 15, 90, 3)

    # convert words to numerical representation
    p, bag = wordToVec(p)
    
    #write all data to single csv file 
    outdir = toCSV(p, True)

    # write of bag of words to json file to reference
    bagstr = json.dumps(bag)
    f = open(os.path.join(outdir, "bagOfWords.json"), 'w')
    f.write(bagstr)
    f.close()

    #write question count of each category to a file
    cat_count(p, outdir)

