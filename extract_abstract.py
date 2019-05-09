import time
import threading 
import pymongo
from libs.textrank import TextRank

BATCH_SIZE = 100000
THREADS = 25

MONGO_CRED = {
    'ip': '127.0.0.1',
    'port': 27017,
    'user': 'root',
    'pwd': 'qwertyui@USTC406',
    'authDB': 'admin',
    'authMethod': 'MONGODB-CR'
}
MONGO_DB_NAME = 'NewsDB-New'
MONGO_DB_COLLECTIONS = ['china', 'finance', 'society', 'technology', 'world']

ASSETS_STOPWORDS_FILE = 'assets/stopwords.txt'


def main():
    mongo_client = pymongo.MongoClient(MONGO_CRED['ip'], MONGO_CRED['port'],
                                        username=MONGO_CRED['user'],
                                        password=MONGO_CRED['pwd'],
                                        authSource=MONGO_CRED['authDB'],
                                        authMechanism=MONGO_CRED['authMethod'])
    mongo_newsdb = mongo_client[MONGO_DB_NAME]

    for coll_name in MONGO_DB_COLLECTIONS:
        print("--- Processing collection '{}'".format(coll_name))
        mongo_coll = mongo_newsdb[coll_name]
        
        for i in range(mongo_coll.estimated_document_count() // BATCH_SIZE + 1):
            articles = list(mongo_coll.find({'abstract': None}).skip(i * BATCH_SIZE).limit(BATCH_SIZE))
            articles_count = len(articles)
            if articles_count == 0:
                break
            threads = init_extract_threads(mongo_coll, articles, THREADS)
            for t in threads:
                t.start()
            tick = time.time()
            while len(articles) > 0:
                time.sleep(10)
                print("Batch {}: {}/{} articles processed in {} seconds"
                        .format(i, articles_count - len(articles), articles_count, time.time() - tick))


def init_extract_threads(mongo_collection, articles, thread_count):
    thread_list = []
    lock = threading.Lock()
    for i in range(thread_count):
        t = threading.Thread(target=extract_thread, args=(mongo_collection, articles, lock))
        thread_list.append(t)
    return thread_list


def extract_thread(mongo_collection, articles, lock):
    tr = TextRank(open(ASSETS_STOPWORDS_FILE, encoding='utf-8'))
    while True:
        lock.acquire()
        article_list = articles[:10]; del articles[:10]
        lock.release()
        if len(article_list) == 0:
            break
        extract(mongo_collection, article_list, tr)


def extract(mongo_collection, articles, tr):
    if len(articles) == 0:
        return False
    for article in articles:
        if 'abstract' in article.keys():
            continue
        tr.sumarize(article['content'])
        abstract = tr.get_key_sentences(3)
        mongo_collection.update_one({'_id': article['_id']}, {'$set': {
            'abstract': abstract
        }}, upsert=False)
    return True


if __name__ == '__main__':
    main()

