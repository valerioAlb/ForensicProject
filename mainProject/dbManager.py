import elasticsearch

class Manager:

    es = elasticsearch.Elasticsearch("127.0.0.1:9200")



    def push(self,index,doc_type,body):

        self.es.index(index=index, doc_type=doc_type, body=body)
        print "Record pushed to elasticsearch"


    def getManager(self):
        return Manager

