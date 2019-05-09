import pymongo
import time

BATCH_SIZE = 100000
COLLECTIONS = ['china', 'finance', 'society', 'technology', 'world']


def main():
    mongo_client = pymongo.MongoClient('127.0.0.1',27017,username='root',password='qwertyui@USTC406',authSource='admin')
    mongo_newsdb_old = mongo_client["NewsDB"]
    mongo_newsdb_new = mongo_client["NewsDB-New"]

    for coll_name in COLLECTIONS:
        print("--- Processing collection '{}'".format(coll_name))

        mongo_coll_old = mongo_newsdb_old[coll_name]
        mongo_coll_new = mongo_newsdb_new[coll_name]

        print("fetching articles from old database", end='    ')
        tick = time.time()
        articles = list(mongo_coll_old.find({}, {'_id': False}))
        print("done in {} seconds".format(time.time() - tick))
        print("sorting articles", end='    ')
        tick = time.time()
        articles.sort(key=lambda elem: elem['time'], reverse=True)
        print("done in {} seconds".format(time.time() - tick))
        print("inserting articles to new database", end='    ')
        tick = time.time()
        mongo_coll_new.insert_many(articles)
        print("done in {} seconds".format(time.time() - tick))


if __name__ == "__main__":
    main()