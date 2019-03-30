'''
@author-name: Rishab Katta

Creating and Loading MongoDB Collections from Postgres IMDB Database. The Program also performs Querying on the loaded
MongoDB Database and Creates indexes to improve the query performance.
'''

from pymongo import MongoClient
import psycopg2
import re
import time
import pymongo



class MongoDBManagement:

    def __init__(self,host,port,pdb,pun,ppwd):
        '''
        Constructor used to connect to both the databases and initialize a MongoDB database

        :param host: Hostname for MongoDB and Postgres databases
        :param port: Port number for MongoDB database
        :param pdb: Postgres database name
        :param pun: Postgres user name
        :param ppwd: Postgres password
        '''
        self.client = MongoClient(host, port)
        self.database = self.client['IMDB']
        self.connection = psycopg2.connect(host=host, database=pdb, user=pun, password=ppwd)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()


    def insert_document(self):
        '''
        Insert values into the MongoDB collection from the values retrieved from Postgres Database
        :return: None
        '''

        self.cursor.execute("SELECT id, name, birthyear, deathyear from member")
        members=self.cursor.fetchall()
        self.collection = self.database['Members']
        for member in members:
            self.memberdoc = {}
            self.memberdoc['_id'] = member[0]
            self.memberdoc['name'] = member[1]
            if member[2] is not None:
                self.memberdoc['birthyear'] = member[2]
            if member[3] is not None:
                self.memberdoc['deathyear'] =member[3]
            self.collection.insert_one(self.memberdoc)



        self.cursor.execute("ALTER TABLE movie "
                            "ALTER COLUMN avgrating TYPE float")

        self.cursor.execute("select m.id,m.type,m.title,m.originaltitle,m.startyear,m.endyear,m.runtime,m.avgrating, m.numvotes, array_agg(DISTINCT g.name),array_agg(DISTINCT md.director), "
                            "array_agg(DISTINCT mw.writer),array_agg(DISTINCT mp.producer), array_agg(DISTINCT amr.actor) "
                            "from movie m inner join movie_genre mg on m.id=mg.movie inner join genre g on mg.genre=g.id inner join movie_director md on m.id=md.movie "
                            "inner join movie_writer mw on mw.movie=m.id inner join movie_producer mp on mp.movie=m.id inner join actor_movie_role amr on amr.movie = m.id "
                            "group by m.id")



        movies=self.cursor.fetchall()
        self.collection = self.database['Movies']

        for movie in movies:
            self.moviedoc={}
            self.moviedoc['_id'] = movie[0]
            if movie[1] is not None:
                self.moviedoc['type'] = movie[1]
            if movie[2] is not None:
                self.moviedoc['title'] = movie[2]
            if movie[3] is not None:
                self.moviedoc['originaltitle']=movie[3]
            if movie[4] is not None:
                self.moviedoc['startyear']= movie[4]
            if movie[5] is not None:
                self.moviedoc['endyear'] = movie[5]
            if movie[6] is not None:
                self.moviedoc['runtime'] = movie[6]
            if movie[7] is not None:
                self.moviedoc['avgrating'] = movie[7]
            if movie[8] is not None:
                self.moviedoc['numvotes'] = movie[8]
            if movie[9] is not None:
                self.moviedoc['genres'] = movie[9]
            if movie[10] is not None:
                self.moviedoc['directors'] = movie[10]
            if movie[11] is not None:
                self.moviedoc['writers'] = movie[11]
            if movie[12] is not None:
                self.moviedoc['producers'] = movie[12]
            if movie[13] is not None:
                amr_list=[]
                for actor in movie[13]:
                    actorroles = {}
                    self.cursor.execute(
                        "select array_agg(r.name) from actor_movie_role amr inner join role r on amr.role = r.id where movie = " + str(movie[0]) + " and actor = " + str(actor))
                    roles = self.cursor.fetchone()
                    actorroles['actor'] = actor
                    actorroles['roles'] = roles[0]
                    amr_list.append(actorroles)

                self.moviedoc['actors'] = amr_list
            self.collection.insert_one(self.moviedoc)

    def nosql_querying(self):
        '''
        Performs Querying on the loaded MongoDB Database
        :return: None
        '''

        self.collection = self.database['Movies']


        regx = re.compile("^Phi.*", re.IGNORECASE)

        pipeline_q1=[{"$unwind": "$actors"},
                    {"$lookup":
                        {"from": "Members",
                        "localField": "actors.actor",
                        "foreignField": "_id",
                        "as": "member_actors"}},
                     {"$match": {"$and": [
                         {"member_actors.name": regx}, {"startyear": {"$ne": 2014}},
                         {"member_actors.deathyear": {"$exists": False}}]}},
                     {"$project": {"_id":0, "member_actors._id": 1}}]

        print("Query1 Results")
        start_time = time.time()
        for doc in (self.collection.aggregate(pipeline_q1)):
            print(doc)
        print("--- %s seconds for 2.1 ---" % (time.time() - start_time))


        regx = re.compile(".*Gill.*", re.IGNORECASE)


        pipeline_q2 = [{"$unwind": "$producers"},
                        {"$lookup":
                            {"from": "Members",
                             "localField": "producers",
                             "foreignField": "_id",
                             "as": "member_producers"}},
                       {"$unwind": "$member_producers"},
                       {"$unwind": "$genres"},

                       {"$match": {"$and": [
                           {"member_producers.name": regx}, {"startyear": {"$eq": 2017}}, {"genres": "Talk-Show"}]}},
                       {"$group": {"_id": "$member_producers._id",
                                   "count": {"$sum": 1}}},

                       {"$match": {"count": {"$gt": 50}}},
                       {"$project": {"member_producers._id": 1}}
                       ]
        print("Query2 Results")
        start_time = time.time()
        for doc in (self.collection.aggregate(pipeline_q2)):
            print(doc)
        print("--- %s seconds for 2.2 ---" % (time.time() - start_time))



        regx = re.compile(".*Bhardwaj.*", re.IGNORECASE)
        pipeline_q3=[{"$unwind": "$writers"},
                        {"$lookup":
                            {"from": "Members",
                             "localField": "writers",
                             "foreignField": "_id",
                             "as": "member_writers"}},
                     {"$match": {
                         "$and": [{"member_writers.name": regx}, {"member_writers.deathyear": {"$exists": False}}]}},
                     {"$group": {"_id": {"writerids": "$member_writers._id"},"avgruntime": {"$avg": "$runtime"}}},
                     {"$project": {"_id": 0, "avgruntime": 1}}
                     ]

        print("Query3 Results")
        start_time = time.time()
        for doc in (self.collection.aggregate(pipeline_q3)):
            print(doc)
        print("--- %s seconds for 2.3 ---" % (time.time() - start_time))

        # Assuming Greatest number is any number greater than 30.

        pipeline_q4=[{"$unwind": "$producers"},
                     {"$lookup":
                            {"from": "Members",
                             "localField": "producers",
                             "foreignField": "_id",
                             "as": "member_producers"}},
                     {"$unwind": "$member_producers"},
                     {"$match": {"$and": [
                         {"member_producers.deathyear": {"$exists": False}}, {"runtime": {"$gt": 120}}]}},
                     {
                         "$group": {
                             "_id": "$member_producers._id",
                             "count": {"$sum": 1}
                         }
                     },{"$match": {"count": {"$gt": 30}}},
                     {"$project": {"_id": 1}}
                     ]

        print("Query4 results")
        start_time = time.time()
        for doc in (self.collection.aggregate(pipeline_q4)):
            print(doc)
        print("--- %s seconds for 2.4 ---" % (time.time() - start_time))

        pipeline_q5=[
            {"$lookup":
                 {"from": "Members",
                  "localField": "directors",
                  "foreignField": "_id",
                  "as": "member_directors"}},
            {   "$unwind": "$actors"},
            {"$lookup":
                 {"from": "Members",
                  "localField": "actors.actor",
                  "foreignField": "_id",
                  "as": "member_actors"}},
            {"$match": {"$and": [
                {"member_directors.name": "James Cameron"}, {"member_actors.name": "Sigourney Weaver"},
                {"genres":"Sci-Fi" }]}},

            {"$project": {"_id": 1, "title":1}}]

        print("Query5 results")
        start_time = time.time()
        for doc in (self.collection.aggregate(pipeline_q5)):
            print(doc)
        print("--- %s seconds for 2.5 ---" % (time.time() - start_time))


    def create_indexes(self):
        '''
        Function used to create indexes on MongoDB Fields
        :return: None
        '''

        self.collection = self.database['Members']

        self.collection.create_index([('name', pymongo.TEXT)], name='search_index', default_language='english')

        self.collection.create_index([('deathyear', pymongo.ASCENDING)])

        self.collection = self.database['Movies']

        self.collection.create_index([('genres', pymongo.ASCENDING)])

        self.collection = self.database['Members']

        self.collection.create_index([('_id', pymongo.ASCENDING)])





if __name__ == '__main__':
    port = int(input("Enter port MongoDB's running on"))
    host = input("Enter host for both MongoDB and Postgres")
    pdb=input("Enter Postgres Database Name")
    pun=input("Enter postgres username")
    ppwd =input("Enter postgres password")


    mongodb =MongoDBManagement(host,port,pdb,pun,ppwd)
    mongodb.insert_document()
    # print("Before Indexing...")
    # mongodb.nosql_querying()
    # mongodb.create_indexes()
    # print("After Indexing...")
    # mongodb.nosql_querying()


