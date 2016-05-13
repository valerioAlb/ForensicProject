import elasticsearch
import logging

class Manager:
    es = elasticsearch.Elasticsearch("127.0.0.1:9200")

    def push(self,index,doc_type,body):

        try:
            self.es.index(index=index, doc_type=doc_type, body=body)
            # es.index(index='forensic_db', doc_type='mails', body=doc)
        except:
            temp = {
                body.keys()[0]:unicode(body[body.keys()[0]], errors='ignore'),
                body.keys()[1]:unicode(body[body.keys()[1]], errors='ignore'),
            }
            self.es.index(index=index, doc_type=doc_type, body=temp)

        #print "Record pushed to elasticsearch"


    def getManager(self):
        return Manager

