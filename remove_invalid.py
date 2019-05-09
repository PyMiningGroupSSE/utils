import pymongo

BATCH_SIZE = 10000
COLLECTIONS = ['china', 'finance', 'society', 'technology', 'world']


def main():
    mongo_client = pymongo.MongoClient('192.168.0.5',8635,username='rwuser',password='qwertyui@USTC406',authSource='admin')
    mongo_newsdb = mongo_client["NewsDB"]

    for coll_name in COLLECTIONS:
        print("--- Processing collection '{}'".format(coll_name))
        mongo_coll = mongo_newsdb[coll_name]
        mongo_coll.delete_many({'content': ''})


if __name__ == '__main__':
    main()
