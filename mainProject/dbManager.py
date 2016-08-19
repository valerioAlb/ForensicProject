import elasticsearch
from elasticsearch import helpers
import utils
import sys

#dbManager works as Singleton, so there is only one istance in the whole program. It is used to upload
#elements into database.

class dbManager(object):

    INSTANCE = None
    es = elasticsearch.Elasticsearch("127.0.0.1:9200")
    tempData = []
    util = utils.utils.get_instance()

    def __init__(self):
        if self.INSTANCE is not None:
            raise ValueError("An instantiation already exist!")


    @classmethod
    def get_instance(cls):
        if cls.INSTANCE is None:
            cls.INSTANCE = dbManager()
        return cls.INSTANCE

    #Method used to push element into ElasticSearch database. It uses helpers.bulk(..) which is very useful
    #to upload more than one information at once.
    def bulk(self,actions):
        if len(actions) > 0:
            self.tempData.extend(actions)

            #If more than # MB, store on elasticsearch and clear list
            if sys.getsizeof(self.tempData) > 1000000:
                print 'UPLOADING ########### The SIZE IS:  ' + str(sys.getsizeof(self.tempData))
                #try:
                helpers.bulk(self.es, self.tempData)
                #except:
                    #print 'Error while uploading : ', actions
                print 'uploaded!'
                self.tempData = []

    #Used in the final part of the program, where should be done the last upload without size constraints.
    def forceBulk(self):
        if len(self.tempData) > 0:
            print 'UPLOADING ############# The SIZE IS:  ' + str(sys.getsizeof(self.tempData))
            helpers.bulk(self.es, self.tempData)
            self.tempData = []

    def initializeDB(self):

        if self.es.indices.exists(self.util.getIndex()):
            print 'The DB already exists'
        else:
            doc = {
                "index": {
                    "analysis": {
                        "analyzer": {
                            "default": {
                                "tokenizer": "standard",
                                "filter": ["my_icu_normalizer", "asciifolding", "lowercase"]
                            }
                        },
                        "filter": {
                            "my_icu_normalizer": {
                                "type": "icu_normalizer",
                                "name": "nfkc",
                                "mode": "decompose"
                            }
                        }
                    }
                }
            }

            res = self.es.indices.create(index=self.util.getIndex(), body=doc)
            print 'Result of operation \"CREATING DATABASE\" :',res